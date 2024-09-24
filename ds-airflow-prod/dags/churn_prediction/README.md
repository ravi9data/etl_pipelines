# Customer Resubscription Prediction Model

This repository contains code for building and applying a machine learning model to predict whether customers are likely to resubscribe. The goal is to identify potential churners and loyal customers to target them with appropriate retention strategies.

## Table of Contents

- [Project Overview](#project-overview)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Usage](#usage)
  - [Training the Model](#training-the-model)
  - [Applying the Model](#applying-the-model)
- [Features](#features)
- [Model Evaluation](#model-evaluation)
- [Business Implications](#business-implications)
- [Datasets & Output](#datasets-&-output)


## Project Overview

This project leverages a RandomForestClassifier to predict customer resubscription likelihood. The model is trained using historical data and then applied to new data to make predictions. The results help in proactive customer retention efforts.

## Project Structure

- `churn_pred_model_final.py`: The main script that trains the model and applies it to new data.
- `README.md`: This readme file.
- `churned_customers_data_20_09_2023.csv`: The dataset used for training the model.
- `Customers_whose_subscription_ending_soon_2023_10_19.csv`: The new dataset for which predictions are made.

## Installation

To run this project, ensure you have the following dependencies installed:


## Usage

## Training the Model
The training process involves data preprocessing, feature selection, model training, and saving the trained model.

### Read the Data:
The dataset churned_customers_data_20_09_2023.csv is read into a pandas DataFrame.
### Data Cleaning:
Handle missing values and convert date columns to pandas DateTime objects.
### Feature Selection:
Select relevant features for prediction.
### Encoding:
Encode categorical features using one-hot encoding and the target variable using LabelEncoder.
### Train-Test Split:
Split the data into training and testing sets.
### Oversampling:
Handle class imbalance using RandomOverSampler.
### Model Training:
Train a RandomForestClassifier on the resampled training data.
### Model Evaluation:
Evaluate the model using classification report and ROC AUC score.
### Save the Model:
Save the trained model to a file.

## Applying the Model
The application process involves loading the trained model, preprocessing new data, making predictions, and saving the results.

### Read New Data:
The new dataset Customers_whose_subscription_ending_soon_2023_10_19.csv is read into a pandas DataFrame.
### Data Cleaning:
Handle missing values and convert date columns to pandas DateTime objects.
### Encoding:
Perform one-hot encoding on categorical features.
### Load Model:
Load the trained model from the file.
### Make Predictions:
Use the model to predict resubscription likelihood for the new data.
### Save Results:
Save the predictions and analyze the results.

## Running the Script
To train the model and apply it to new data, run the train_and_apply_model.py script:

``` bash
python3 churn_pred_model_final.py
```
### Features

Selected Features for Prediction
Subscription value
Subscriptions per customer
Has other rental
Start month
Cancellation month
Total customer revenue
Order keeping duration
Rental period
Cancellation Reason
Category name
Age
Target Variable
did_resubscribe
Model Evaluation

### Precision: Class 0 (84%), Class 1 (86%)
### Recall: Class 0 (81%), Class 1 (89%)
### ROC AUC Score: 0.85

The model demonstrates high accuracy in predicting customer resubscription likelihood, making it a valuable tool for customer retention strategies.

### Business Implications

### Targeted Actions:
Precisely identify potential churners and loyal customers for targeted actions.
### Optimize Retention Efforts:
Enhance overall satisfaction through optimized customer retention efforts.
### Data-Driven Decisions:
Make informed decisions for long-term business planning.


### Datasets & Output

https://drive.google.com/drive/folders/13rpeO_OeH8rZ5-LiLeqvkCVlBztoLAip?usp=sharing
