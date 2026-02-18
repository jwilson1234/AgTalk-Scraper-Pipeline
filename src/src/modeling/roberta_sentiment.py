"""
RoBERTa sentiment pipeline for AgTalk data

Description:
    Filters precision-agriculture posts and performs batched sentiment using
    HuggingFace RoBERTa model

Output: 
    - negative probability
    - neutral probability
    - positive probability
    - sentiment label
    - sentiment score
"""

# Standard Library Imports

import re

# Third-Party Imports

import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from tqdm import tqdm

# Configuration

INPUT_PATH = "data/processed/machinery_talk_cleaned.csv"
OUTPUT_PATH = "data/processed/machinery_talk_sentiment.csv"

MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment-latest"
BATCH_SIZE = 16

# Precision Keyword Filter

def filter_precision_posts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters posts related to precision agriculture using keyword matching

    Returns:
        Filtered dataframe that contains only precision-related posts
    """
    precision_keywords = [
        "variable rate",
        "VRT",
        "prescription map",
        "yield monitor",
        "yield map",
        "section control",
        "rate controller",
        "RTK",
        "autosteer",
        "precision planter",
        "grid sampling",
        "variable population",
        "overlap control"
    ]
    # Building case-insensitive regex pattern

    pattern = r"\b(" + "|".join(map(re.escape, precision_keywords)) + r")\b"

    # Identifying precision-related posts

    mask = df["clean_text"].str.contains(
        pattern,
        case=False,
        na=False,
        regex=True
    )

    # Filtering and resetting index
    filtered_df = df[mask].copy().reset_index(drop=True)

    return filtered_df


# Model Loader

def load_model(model_name: str):

    """
    Loads tokenizer and model, moves model to device
    
    Returns:
        - tokenizer
        - model
        - device
    """
    
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(model_name)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)
    model.eval()

    return tokenizer, model, device

# Sentiment Inference

def predict_sentiment(
    texts: list[str],
    tokenizer,
    model,
    device,
    batch_size: int
) -> pd.DataFrame:
    
    """
    Runs batched sentiment inference

    Returns:
        probabilities (negative, neutral, positive)
    """
  
    results = []

    for i in tqdm(range(0, len(texts), batch_size)):
        batch = texts[i:i + batch_size]

        inputs = tokenizer(
            batch,
            padding=True,
            truncation=True,
            max_length=512,
            return_tensors="pt"
        )

        inputs = {k: v.to(device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = model(**inputs)
            probs = torch.nn.functional.softmax(outputs.logits, dim=1)

        results.extend(probs.cpu().numpy())

    sentiment_df = pd.DataFrame(results)
    sentiment_df.columns = ["negative", "neutral", "positive"]

    return sentiment_df

# main block

if __name__ == "__main__":

    tokenizer, model, device = load_model(MODEL_NAME)
    
    # Loading dataset
    df = pd.read_csv(INPUT_PATH)

    # Filtering precision-related posts
    df = filter_precision_posts(df)
    print(f"Posts after precision filter: {len(df)}")

    if df.empty:
        print("No precision-related posts found. Exiting.")
        exit()

    sentiment_df = predict_sentiment(
        texts=df["clean_text"].astype(str).tolist(),
        tokenizer=tokenizer,
        model=model,
        device=device,
        batch_size=BATCH_SIZE
    )

    # Merging predictions with original dataframe
    df = pd.concat([df.reset_index(drop=True), sentiment_df], axis=1)

    # Deriving sentiment label
    df["sentiment_label"] = sentiment_df.idxmax(axis=1)

    # Creating weighted sentiment score (-1 to +1 scale)
    df["sentiment_score"] = (
        df["negative"] * -1 +
        df["neutral"] * 0 +
        df["positive"] * 1
    )

    # Saving results
    df.to_csv(OUTPUT_PATH, index=False)

    print("Sentiment inference complete")
