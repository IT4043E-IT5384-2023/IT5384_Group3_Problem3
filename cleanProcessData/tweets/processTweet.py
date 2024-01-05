import pandas as pd
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.util import ngrams

# Download NLTK resources
import nltk

def process_text(text, tweet_id):
    # Tokenize the text
    words = word_tokenize(text.lower())  # Convert to lowercase for consistency

    # Remove stop words
    stop_words = set(stopwords.words('english'))
    words = [word for word in words if word.isalnum() and word not in stop_words]

    # Remove numeric words
    words = [word for word in words if not word.isdigit()]

    # Stemming
    stemmer = PorterStemmer()
    words = [stemmer.stem(word) for word in words]

    return [{'Key': tweet_id, 'Type': 'Word', 'Content': word} for word in words]

def extract_ngrams(text, tweet_id, n):
    words = word_tokenize(text.lower())
    n_grams = list(ngrams(words, n))
    return [{'Key': tweet_id, 'Type': f'{n}-gram', 'Content': ' '.join(gram)} for gram in n_grams]

# Load your data
df = pd.read_csv('tweetData.csv')  # Replace 'your_tweet_data.csv' with your actual file

# Process each text-based field
processed_data = []

for index, row in df.iterrows():
    tweet_id = row['id_str']
    words = process_text(row['rawContent'], tweet_id)

    processed_data.extend(words)

    n = 2  # You can change n to any desired value for n-grams
    n_grams = extract_ngrams(row['rawContent'], tweet_id, n)

    processed_data.extend(n_grams)

# Create a DataFrame from the processed data
processed_df = pd.DataFrame(processed_data)

# Assign a unique sequential identifier to each word/n-gram
processed_df['Sequential_ID'] = range(1, len(processed_df) + 1)

# Output the processed data to CSV
processed_df.to_csv('processed_tweet_data.csv', index=False)
