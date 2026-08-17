# nutrition-tracker
# 🍏 Offline AI Nutrition & Macro Tracker

A privacy-first, highly accurate nutrition tracking system that combines the flexibility of natural language processing and computer vision with the mathematical reliability of deterministic database lookups. 

This project allows users to seamlessly log meals using text or images while ensuring macro calculations remain 100% accurate by relying on authoritative nutritional datasets rather than AI-generated math.

## ✨ Features

* **Natural Language Parsing:** Log meals intuitively using everyday language, powered by local Large Language Models (LLMs).
* **Image-Based Food Scanning:** Quickly snap a picture of a meal to identify and log food items via the mobile app.
* **Deterministic Accuracy:** Prevents AI math hallucinations by using LLMs strictly for entity extraction and parsing, while routing all actual macro calculations through a reliable database.
* **Massive Offline Database:** Operates on a fully relational SQLite database pre-loaded with authoritative USDA data (Foundation, SR Legacy, and FNDDS).
* **Branded Food Lookups:** Integrates Open Food Facts data for scanning and identifying specific packaged goods.
* **Multi-Platform Access:** Features a React Native mobile application for on-the-go scanning and a web dashboard for reviewing daily macro trends and detailed analytics.

## 🛠️ Tech Stack

**Backend & API**
* **Framework:** FastAPI (Python)
* **Database:** SQLite (Relational)
* **AI/ML:** Local Large Language Models (LLMs) for offline, private natural language processing

**Frontend**
* **Mobile App:** React Native (iOS & Android)
* **Web Dashboard:** (Insert your frontend web framework here, e.g., React/Next.js)

**Data Sources**
* USDA Foundation Foods
* USDA SR Legacy
* USDA FNDDS (Food and Nutrient Database for Dietary Studies)
* Open Food Facts (for branded/packaged items)

## 🏗️ Architecture overview

1. **Input Layer:** The user submits a food entry via text (Web/Mobile) or image scan (Mobile).
2. **AI Parsing (Local LLM):** The local model processes the input to extract key entities (e.g., "Food: Chicken Breast", "Quantity: 200", "Unit: grams").
3. **Database Routing:** The FastAPI backend takes the extracted entities and queries the SQLite database.
4. **Deterministic Calculation:** The system retrieves the exact nutritional values from the USDA/Open Food Facts tables and computes the total macros.
5. **Presentation Layer:** The calculated, accurate data is logged and returned to the user's web dashboard or mobile app.

## 🚀 Getting Started

### Prerequisites
* Python 3.9+
* Node.js & npm/yarn (for React Native/Web frontend)
* SQLite3

### Backend Setup
1. Clone the repository:
   ```bash
   git clone [https://github.com/yourusername/nutrition-tracker.git](https://github.com/yourusername/nutrition-tracker.git)
   cd nutrition-tracker/backend
