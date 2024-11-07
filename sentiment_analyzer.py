import pandas as pd
import string
from deep_translator import GoogleTranslator
from langdetect import detect
from nltk import pos_tag
from nltk.corpus import stopwords, wordnet
from nltk.stem import WordNetLemmatizer
from pymongo import MongoClient
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from tenacity import retry, stop_after_attempt, wait_fixed

# Initialize Sentiment Analyzer
sentiments = SentimentIntensityAnalyzer()

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017")
databases = ["traveloka_homestays"]
translator = GoogleTranslator(source='auto', target='en')
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def translate_batch_with_retry(batch):
    return translator.translate_batch(batch)
# Function to return the WordNet object value
def get_wordnet_pos(pos_tag):
    if pos_tag.startswith("J"):
        return wordnet.ADJ
    elif pos_tag.startswith("V"):
        return wordnet.VERB
    elif pos_tag.startswith("N"):
        return wordnet.NOUN
    elif pos_tag.startswith("R"):
        return wordnet.ADV
    else:
        return wordnet.NOUN


# Function to clean text
def clean_text(text):
    if text is None:
        return ""
    # Remove \t
    text = text.replace("\t", " ")
    # Remove \n
    text = text.replace("\n", " ")
    # Lowercase text
    text = text.lower()
    # Tokenize text and remove punctuation
    text = [word.strip(string.punctuation) for word in text.split(" ")]
    # Remove words that contain numbers
    text = [word for word in text if not any(c.isdigit() for c in word)]
    # Remove stop words
    stop_words = stopwords.words("english")
    text = [x for x in text if x not in stop_words]
    # Remove empty tokens
    text = [t for t in text if len(t) > 0]
    # POS tag text
    pos_tags = pos_tag(text)
    # Lemmatize text
    text = [
        WordNetLemmatizer().lemmatize(t[0], get_wordnet_pos(t[1])) for t in pos_tags
    ]
    # Remove words with only one letter
    text = [t for t in text if len(t) > 1]
    # Join all
    text = " ".join(text)
    return text


# Function to train LDA model and get topics
def train_lda_model(reviews):
    # Vectorize the preprocessed reviews
    vectorizer = CountVectorizer(max_features=1000, lowercase=True)
    X = vectorizer.fit_transform(reviews)

    # Train the LDA model with num_topics = 5
    num_topics = 5
    lda_model = LatentDirichletAllocation(
        n_components=num_topics, max_iter=10, learning_method="online", random_state=42
    )
    lda_model.fit(X)

    # Get the most representative words for each topic
    feature_names = vectorizer.get_feature_names_out()
    topic_words = [
        [feature_names[i] for i in topic.argsort()[:-11:-1]]
        for topic in lda_model.components_
    ]

    # Mapping of topics to keywords
    topic_keywords_mapping = {}

    # Iterate over the most representative words for each topic
    for i, words in enumerate(topic_words):
        topic_keywords_mapping[f"Topic {i}"] = words

    return topic_keywords_mapping

def automatic_labeling(lda_topics, predefined_keywords):
    topic_labels = {}
    for topic, words in lda_topics.items():
        matched_keywords = []
        for keyword, keyword_list in predefined_keywords.items():
            if any(word in keyword_list for word in words):
                matched_keywords.append(keyword)
        if matched_keywords:
            topic_labels[topic] = " ".join(matched_keywords)
        else:
            topic_labels[topic] = "Uncategorized"
    return topic_labels


def assign_topic_label(review, lda_topics):
    max_topic = None
    max_match_count = 0

    # Đếm số lần xuất hiện của các từ khóa của mỗi chủ đề trong đánh giá
    for topic, words in lda_topics.items():
        match_count = sum(1 for word in words if word in review)
        if match_count > max_match_count:
            max_match_count = match_count
            max_topic = topic

    return max_topic


# Iterate through databases
for database in databases:
    print("Getting data from " + database)
    db = client[database]
    collection = db["reviews"]
    df = pd.DataFrame(list(collection.find()))

    # Filter out empty or non-existent reviews
    df = df[
        (df["review_content"] != "There are no comments available for this review")
        & df["review_content"].notna()
    ]
    # Replace comma with dot in review_score column
    df["review_score"] = df["review_score"].apply(
        lambda x: x.replace(",", ".") if "," in str(x) else x
    )

    # Translate reviews
    print("Translating reviews")
    batch_size = 500
    for i in range(0, len(df), batch_size):
        df_subset = df.iloc[i : i + batch_size].copy()
        df_subset["en_review"] = ""
        batch = df_subset["review_content"].astype(str).tolist()
        print("Batch: ", i)
        translated_reviews = translate_batch_with_retry(batch)
        df_subset.loc[:, "en_review"] = translated_reviews


        # Clean text
        print("Cleaning reviews")
        df_subset.loc[:, "clean_review"] = df_subset["en_review"].apply(clean_text)

        # Train LDA model
        print("Training LDA model")
        lda_topics = train_lda_model(df_subset["clean_review"].tolist())

        # Sentiment analysis
        print("Sentiment analyzing")
        df_subset["sentiments"] = df_subset["clean_review"].apply(
            lambda x: sentiments.polarity_scores(x)
        )
        df_subset = pd.concat(
            [
                df_subset.drop(["sentiments"], axis=1),
                df_subset["sentiments"].apply(pd.Series),
            ],
            axis=1,
        )
        df_subset.loc[:, "sentiment"] = df_subset.apply(
            lambda row: (
                "positive"
                if row["pos"] > row["neg"] and float(row["review_score"]) >= 6.0
                else (
                    "negative"
                    if row["neg"] > row["pos"] or float(row["review_score"]) <= 6.0
                    else "neutral"
                )
            ),
            axis=1,
        )
        predefined_keywords = {
            "location": ["location", "place", "area", "room", "home", "homestay"],
            "amenity": ["amenity", "facility", "service", "equipment"],
            "host": ["host", "owner", "manager", "staff"],
            "noise": ["noise", "sound", "quiet", "noisy"],
            "cleanliness": ["cleanliness", "clean", "tidy", "dirty"],
        }

        for topic in lda_topics.items():
            print(topic)
        
        topic_labels = automatic_labeling(lda_topics, predefined_keywords)
        print(topic_labels)
        for index, row in df_subset.iterrows():
            review = row["clean_review"]
            topic_label = assign_topic_label(review, lda_topics)
            label_name = topic_labels.get(topic_label, "Unknown")
            df_subset.at[index, "label"] = label_name

        # Update MongoDB
        print("Updating MongoDB")
        columns_to_update = ["sentiment", "label"]
        for index, row in df_subset.iterrows():
            document_id = row["_id"]
            update_data = {column: row[column] for column in columns_to_update}
            collection.update_one(
                {"_id": document_id}, {"$set": update_data}, upsert=True
            )
