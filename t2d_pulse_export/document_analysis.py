"""
Document Processing and Sentiment Analysis Module
Supports text extraction from various document formats and basic financial sentiment analysis.
"""

import os
import re
import io
import base64
from datetime import datetime
import pandas as pd
import numpy as np

# Document processing libraries
import docx
from PyPDF2 import PdfReader
import openpyxl

# We'll use a simpler sentiment analysis approach for now
# This can be expanded to use FinBERT in a future update

# Dictionary of financial positive and negative terms
# Based on common financial sentiment lexicons
FINANCIAL_POSITIVE_TERMS = [
    "growth", "profit", "increase", "gain", "improved", "strong", "positive", 
    "opportunity", "exceed", "beat", "surpass", "above", "robust", "success",
    "advantage", "favorable", "efficient", "innovation", "momentum", "upward",
    "outperform", "strength", "confident", "achieved", "record", "sustainable",
    "expansion", "recovery", "breakthrough", "leading", "optimistic", "progress",
    "accelerate", "impressive", "resilient", "beat expectations", "cost-effective"
]

FINANCIAL_NEGATIVE_TERMS = [
    "loss", "decline", "decrease", "fall", "risk", "below", "weak", "negative",
    "challenge", "miss", "fail", "down", "poor", "underperform", "concern",
    "unfavorable", "inefficient", "uncertain", "downward", "slowdown", "obstacle",
    "behind", "disappointing", "crisis", "struggle", "volatility", "deficit",
    "contraction", "recession", "threat", "cautious", "lag", "unexpected",
    "underperform", "miss expectations", "weakness", "disruption", "challenging"
]

def extract_text_from_docx(file_content):
    """Extract text from a .docx file"""
    try:
        document = docx.Document(io.BytesIO(file_content))
        text = "\n".join([para.text for para in document.paragraphs])
        return text
    except Exception as e:
        print(f"Error extracting text from DOCX: {e}")
        return ""

def extract_text_from_pdf(file_content):
    """Extract text from a PDF file"""
    try:
        pdf = PdfReader(io.BytesIO(file_content))
        text = ""
        for page in pdf.pages:
            text += page.extract_text() + "\n"
        return text
    except Exception as e:
        print(f"Error extracting text from PDF: {e}")
        return ""

def extract_text_from_txt(file_content):
    """Extract text from a plain text file"""
    try:
        return file_content.decode('utf-8')
    except Exception as e:
        print(f"Error extracting text from TXT: {e}")
        return ""

def analyze_document_sentiment(text):
    """
    Analyze sentiment of financial text using a rule-based lexicon approach.
    
    This function uses financial-specific positive and negative term lists 
    to categorize sentiment in the document. It returns normalized scores 
    for positive, negative, and neutral sentiment, along with an overall
    sentiment label and score (0-100).
    """
    # Handle empty text
    if not text or text.strip() == "":
        return {"positive": 0, "negative": 0, "neutral": 1, "label": "neutral", "score": 50}
    
    try:
        # Convert to lowercase for case-insensitive matching
        text_lower = text.lower()
        
        # Initialize counters for sentiment analysis
        positive_count = 0
        negative_count = 0
        total_words = len(text_lower.split())
        
        # Process text in chunks for better memory management
        chunks = split_text_into_chunks(text)
        
        # Match full words only (not parts of words)
        for chunk in chunks:
            chunk_lower = chunk.lower()
            words = chunk_lower.split()
            
            # Count positive terms
            for term in FINANCIAL_POSITIVE_TERMS:
                # If term has multiple words
                if ' ' in term:
                    positive_count += chunk_lower.count(term.lower())
                else:
                    # Count only full word matches
                    for word in words:
                        word_clean = word.strip('.,;:()[]{}"\'-')
                        if word_clean == term.lower():
                            positive_count += 1
            
            # Count negative terms
            for term in FINANCIAL_NEGATIVE_TERMS:
                # If term has multiple words
                if ' ' in term:
                    negative_count += chunk_lower.count(term.lower())
                else:
                    # Count only full word matches
                    for word in words:
                        word_clean = word.strip('.,;:()[]{}"\'-')
                        if word_clean == term.lower():
                            negative_count += 1
        
        # Calculate sentiment metrics
        total_sentiment_matches = positive_count + negative_count
        
        # Calculate sentiment scores with normalization
        if total_sentiment_matches > 0:
            # Raw sentiment scores based on term frequency
            positive_score = positive_count / total_sentiment_matches
            negative_score = negative_count / total_sentiment_matches
            
            # Determine neutrality based on total matches vs. document size
            match_ratio = total_sentiment_matches / max(1, total_words)
            
            # Lower match ratio means more neutral content
            neutral_score = max(0, 1 - (match_ratio * 10))  # Scale to make small ratios more meaningful
            neutral_score = min(0.8, neutral_score)  # Cap neutrality at 0.8 to prevent all neutral results
            
            # Adjust positive and negative to account for neutrality
            adjustment = (1 - neutral_score) / (positive_score + negative_score)
            positive_score = positive_score * adjustment
            negative_score = negative_score * adjustment
        else:
            # No sentiment terms found - mostly neutral
            positive_score = 0
            negative_score = 0
            neutral_score = 1
        
        # Create sentiment scores dictionary
        sentiment_scores = {
            "positive": positive_score,
            "negative": negative_score,
            "neutral": neutral_score,
            "term_matches": {
                "positive_terms": positive_count,
                "negative_terms": negative_count,
                "total_words": total_words
            }
        }
        
        # Determine overall sentiment label
        if positive_score > negative_score and positive_score > neutral_score:
            sentiment_label = "positive"
        elif negative_score > positive_score and negative_score > neutral_score:
            sentiment_label = "negative"
        else:
            sentiment_label = "neutral"
        
        # Calculate a score from 0-100 (0 = very negative, 100 = very positive)
        if positive_score + negative_score > 0:
            sentiment_value = 50 + (
                50 * ((positive_score - negative_score) / (positive_score + negative_score))
            )
        else:
            sentiment_value = 50  # Perfectly neutral
        
        # Ensure the score is between 0 and 100
        sentiment_value = min(100, max(0, sentiment_value))
        
        # Add sentiment label and score to results
        sentiment_scores["label"] = sentiment_label
        sentiment_scores["score"] = sentiment_value
        
        return sentiment_scores
    
    except Exception as e:
        print(f"Error analyzing sentiment: {e}")
        return {"positive": 0, "negative": 0, "neutral": 1, "label": "neutral", "score": 50}

def split_text_into_chunks(text, chunk_size=512):
    """Split text into chunks of approximately chunk_size"""
    # Split by sentences and then recombine to chunks
    sentences = re.split(r'(?<=[.!?])\s+', text)
    chunks = []
    current_chunk = ""
    
    for sentence in sentences:
        if len(current_chunk) + len(sentence) <= chunk_size:
            current_chunk += sentence + " "
        else:
            chunks.append(current_chunk.strip())
            current_chunk = sentence + " "
    
    if current_chunk:
        chunks.append(current_chunk.strip())
        
    return chunks

def extract_qa_section(text):
    """
    Extract Q&A section from earnings call transcript if it exists
    Note: This is no longer required as we analyze the entire document.
    Kept for backward compatibility.
    """
    # Simply return the full text - no need to search for Q&A sections
    return text

def process_document(content, filename):
    """
    Process uploaded document and extract text based on file type
    """
    file_ext = os.path.splitext(filename.lower())[1]
    
    # Extract text based on file type
    if file_ext == '.docx':
        text = extract_text_from_docx(content)
    elif file_ext == '.pdf':
        text = extract_text_from_pdf(content)
    elif file_ext == '.txt':
        text = extract_text_from_txt(content)
    else:
        # Unsupported file type
        return {
            "status": "error",
            "message": f"Unsupported file type: {file_ext}. Please upload .docx, .pdf, or .txt files."
        }
    
    # Check if text was extracted successfully
    if not text:
        return {
            "status": "error",
            "message": f"Failed to extract text from {filename}."
        }
    
    # Analyze the full document without Q&A section extraction
    full_sentiment = analyze_document_sentiment(text)
    
    # Use the full text sentiment score as the overall score
    overall_score = full_sentiment["score"]
    
    # Prepare result
    result = {
        "status": "success",
        "filename": filename,
        "text_length": len(text),
        "qa_section_found": False,  # Always false since we don't search for Q&A
        "qa_section_length": 0,
        "full_text_sentiment": full_sentiment,
        "qa_sentiment": None,  # No Q&A sentiment since we don't extract it
        "overall_score": min(100, max(0, overall_score)),
        "processed_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    return result

def document_result_to_dataframe(result):
    """
    Convert document analysis result to a pandas DataFrame with date
    for integration with dashboard
    """
    if result["status"] != "success":
        return pd.DataFrame()
    
    # Create a one-row DataFrame with the sentiment score and current date
    df = pd.DataFrame({
        'date': [datetime.now()],
        'value': [result["overall_score"]]
    })
    
    return df
