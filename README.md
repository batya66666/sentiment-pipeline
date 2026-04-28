Sentiment Analysis Pipeline

Machine Learning pipeline for sentiment analysis of tweets.
The project processes text data, applies preprocessing, and predicts the sentiment of messages using a trained machine learning model.

Sentiment analysis is a Natural Language Processing (NLP) technique that automatically determines whether a piece of text expresses positive, negative, or neutral sentiment.

Project Overview

This project implements a complete machine learning pipeline for sentiment classification.

The pipeline includes:

Data preprocessing
Feature extraction
Model training
Model evaluation
Inference on new text data

The system is designed to analyze tweets and determine their sentiment polarity.

Project Structure
sentiment-pipeline
│
├── app/
│   └── real_tweets.csv        # dataset (ignored in repo)
│
├── configs/
│
├── models/
│   └── sentiment_logreg_model
│
├── metrics.json               # evaluation metrics
│
├── docker-compose.yml
│
└── .gitignore

Description:

app/ – contains data used for training and testing
configs/ – configuration files for the pipeline
models/ – trained machine learning models
metrics.json – model evaluation results
docker-compose.yml – environment setup for running the project
.gitignore – ignored files
Machine Learning Pipeline

The sentiment pipeline consists of several stages:

1. Data Preprocessing

Text cleaning includes:

removing punctuation
converting text to lowercase
removing stopwords
tokenization
2. Feature Extraction

The text is transformed into numerical features using vectorization techniques such as:

Bag of Words
TF-IDF
3. Model Training

The project uses a Logistic Regression model for sentiment classification.

The model learns patterns in text to predict whether a tweet is:

Positive
Negative
Neutral
4. Model Evaluation

The trained model is evaluated using common ML metrics such as:

Accuracy
Precision
Recall
F1-score

Evaluation results are stored in:

metrics.json
Installation

Clone the repository:

git clone https://github.com/batya66666/sentiment-pipeline.git
cd sentiment-pipeline

Create a virtual environment:

python -m venv venv

Activate the environment:

Windows

venv\Scripts\activate

Mac / Linux

source venv/bin/activate

Install dependencies:

pip install -r requirements.txt
Running the Project

Run the sentiment pipeline:

python main.py

Or run using Docker:

docker-compose up --build
Example

Input:

I love this product, it works perfectly!

Output:

Sentiment: Positive
Dataset

The dataset contains real tweets used for training the sentiment model.

The dataset is not included in the repository due to file size limitations.

Technologies Used
Python
Machine Learning
NLP
Scikit-learn
Docker
Pandas
NumPy
Future Improvements

Possible improvements for the project:

Use deep learning models (BERT / RoBERTa)
Add REST API for predictions
Deploy the model as a web service
Improve dataset size and diversity
