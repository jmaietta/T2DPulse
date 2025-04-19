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
    Analyze sentiment of financial text using basic keyword approach.
    Returns sentiment scores for positive, negative, and neutral.
    
    This is a simplified version that can be upgraded to FinBERT in the future.
    """
    # Handle empty text
    if not text or text.strip() == "":
        return {"positive": 0, "negative": 0, "neutral": 1, "label": "neutral", "score": 50}
    
    try:
        # Convert to lowercase for case-insensitive matching
        text_lower = text.lower()
        
        # Count positive and negative terms
        positive_count = 0
        negative_count = 0
        
        # Process text in chunks for better memory management
        chunks = split_text_into_chunks(text)
        
        for chunk in chunks:
            chunk_lower = chunk.lower()
            
            # Count positive terms
            for term in FINANCIAL_POSITIVE_TERMS:
                positive_count += chunk_lower.count(term.lower())
            
            # Count negative terms
            for term in FINANCIAL_NEGATIVE_TERMS:
                negative_count += chunk_lower.count(term.lower())
        
        # Calculate total matches
        total_count = positive_count + negative_count
        
        # Calculate sentiment scores
        if total_count > 0:
            positive_score = positive_count / total_count
            negative_score = negative_count / total_count
            neutral_score = 1 - (positive_score + negative_score)
            
            # Ensure neutral score is not negative
            neutral_score = max(0, neutral_score)
            
            # Normalize scores to sum to 1
            total_scores = positive_score + negative_score + neutral_score
            if total_scores > 0:
                positive_score = positive_score / total_scores
                negative_score = negative_score / total_scores
                neutral_score = neutral_score / total_scores
        else:
            # No sentiment terms found
            positive_score = 0
            negative_score = 0
            neutral_score = 1
        
        # Create sentiment scores dictionary
        sentiment_scores = {
            "positive": positive_score,
            "negative": negative_score,
            "neutral": neutral_score
        }
        
        # Determine overall sentiment label
        if positive_score > negative_score and positive_score > neutral_score:
            sentiment_label = "positive"
        elif negative_score > positive_score and negative_score > neutral_score:
            sentiment_label = "negative"
        else:
            sentiment_label = "neutral"
        
        # Calculate a score from 0-100 (0 = very negative, 100 = very positive)
        sentiment_value = 50 + (
            50 * ((positive_score - negative_score) / max(1, positive_score + negative_score))
        )
        
        # Ensure the score is between 0 and 100
        sentiment_value = min(100, max(0, sentiment_value))
        
        # Combine all results
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
    Extract Q&A section from earnings call transcript
    This is a simple implementation and may need refinement based on transcript formats
    """
    # Look for common Q&A section markers
    qa_markers = [
        r'Question-and-Answer Session',
        r'Questions and Answers',
        r'Q&A Session',
        r'Q & A'
    ]
    
    qa_text = ""
    
    # Try to find Q&A section
    for marker in qa_markers:
        match = re.search(f"({marker})(.*?)(Conference Call|$)", text, re.DOTALL | re.IGNORECASE)
        if match:
            qa_text = match.group(2)
            break
    
    # If no marker found, look for Q: pattern
    if not qa_text:
        # Find all Q: and A: patterns
        qa_parts = re.findall(r'(?:^|\n)(?:Q:|Question:).*?(?=(?:^|\n)(?:Q:|Question:|$))', text, re.DOTALL | re.MULTILINE)
        qa_text = '\n'.join(qa_parts)
    
    # If still no QA text found, return empty string
    if not qa_text:
        return ""
        
    return qa_text

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
    
    # Extract Q&A section if it exists
    qa_text = extract_qa_section(text)
    
    # Analyze sentiment on full text
    full_sentiment = analyze_document_sentiment(text)
    
    # Analyze sentiment on Q&A section if available
    qa_sentiment = None
    if qa_text:
        qa_sentiment = analyze_document_sentiment(qa_text)
    
    # Calculate overall sentiment score
    # If Q&A section was found, weigh it more heavily (70% Q&A, 30% full text)
    if qa_sentiment:
        overall_score = (qa_sentiment["score"] * 0.7) + (full_sentiment["score"] * 0.3)
    else:
        overall_score = full_sentiment["score"]
    
    # Prepare result
    result = {
        "status": "success",
        "filename": filename,
        "text_length": len(text),
        "qa_section_found": bool(qa_text),
        "qa_section_length": len(qa_text) if qa_text else 0,
        "full_text_sentiment": full_sentiment,
        "qa_sentiment": qa_sentiment,
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