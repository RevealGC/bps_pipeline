"""
cm_cct.py
"""

# %% 
import pandas as pd
import nltk
import re
import os
import pickle

nltk.download('punkt')
nltk.download('punkt_tab')
nltk.download('wordnet')
nltk.download('stopwords')
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.linear_model import LogisticRegression
from scipy.stats import chi2_contingency


def clean_text_nltk(text: str) -> str:
    stop_words = set(stopwords.words('english'))
    lemmatizer = WordNetLemmatizer()

    if not isinstance(text, str):
        text = "" if text is None else str(text)
        
    text = text.lower()

    text = re.sub(r"[^\w\s]", " ", text)

    tokens = word_tokenize(text)
    tokens = [t for t in tokens if t not in stop_words]
    tokens = [lemmatizer.lemmatize(t) for t in tokens]
    #    Note: consider using part-of-speech tagging?

    cleaned_text = " ".join(tokens)

    return cleaned_text

def apply_pkl_model(
        permit_df: pd.DataFrame,
        pkl_path:str,
        infield_name:str,
        outfield_name = None  # Optional, if None will default to "predicted"
        ) -> pd.DataFrame:
    """
    Apply a pickled model to permit data.

    Args:
        permit_df (pd.DataFrame): Input DataFrame containing permit data.
        name (str): Model name.
        feature (str): Feature to predict.
        city (str): City identifier.

    Returns:
        pd.DataFrame: DataFrame with predictions.
    """
    if outfield_name is None:
        out_name = "predicted"
    else:
        out_name = outfield_name

    df = permit_df.copy()

    
    with open(pkl_path, "rb") as f:
        model = pickle.load(f)
    
    predicted = model.predict(df[infield_name])
    df_predicted = pd.DataFrame(predicted, columns=[out_name])

    return pd.concat([permit_df, df_predicted], axis=1)

def train_data(
        training_df: pd.DataFrame, 
        model_name:str, 
        feature:str,
        input_field:str,
        sk_pipeline: Pipeline,
        write_pkl: str = None,
        subset: str = None,
        subset_field: str = None
    ) -> pd.DataFrame:
    """
    Preprocess training data for text classification.

    Args:
        df (pd.DataFrame): Input DataFrame containing training data.

    Returns:
        pd.DataFrame: DataFrame with processed text.
    """
    train_sub = training_df.copy()
    if subset:
        if subset_field is None:
            raise ValueError("subset_field must be provided if subset is specified")
        
        train_sub = train_sub[train_sub[subset_field].str.lower() == subset.lower()]
    train_sub = train_sub[[input_field, feature]].dropna()
    
    X_train = train_sub[input_field]
    y_train = train_sub[feature]

    fitted_model = sk_pipeline.fit(X_train, y_train)

    if write_pkl:  # Save the fitted model to a file
        try:
            with open(write_pkl, 'wb') as f:
                pickle.dump(fitted_model, f)
        except Exception as e:
            print(f"Failed to save the model to {write_pkl}. Please check the path.")
            raise e

    return fitted_model

def refit_nlp_pipeline(df:pd.DataFrame,PKL_MODEL_PATH) -> None:
    """
    Refit the NLP pipeline for each feature and save the fitted models.
    """
    df_train = df.copy()

    pipeline = Pipeline([
            ("vect", CountVectorizer()),
            ("tfidf", TfidfTransformer()),
            ("clf", LogisticRegression(multi_class="auto"))
        ])
    features = ["Category", "Type", "Class"]
    subsets = ["Chicago", None]
    mname = "bpsnlpmodel"

    for feature in features:
        filename = f"{mname}_{feature}"
        for subset in subsets:
            if subset is None:
                outpath = os.path.join(PKL_MODEL_PATH, f"{filename}_fitted.pkl")
            else:
                outpath = os.path.join(PKL_MODEL_PATH, f"{filename}_{subset}_fitted.pkl")

            fitted_model = train_data(
                df_train,
                mname,
                feature,
                "DESCRIPTION_processed",
                pipeline,
                write_pkl=outpath,
                subset=subset,  # Use subset for city filtering if needed
                subset_field="CM Juris Name" if subset else None  # Only used if subset is specified
            )

def refit_main():
    PKL_MODEL_PATH = "C:/Users/ndece/Github/bps_pipeline/pkl/"
    TRAINING_DATA_PATH = "C:/Users/ndece/Github/bps_pipeline/.training_data/"
    df_tr = pd.read_csv(os.path.join(TRAINING_DATA_PATH, "dctd_cm_annaheim_oceancity_chicago_fargo.csv"), encoding="iso-8859-1")
    refit_nlp_pipeline(df_tr,PKL_MODEL_PATH)

def preprocess_comb_desc(permit_df: pd.DataFrame,PKL_MODEL_PATH) -> pd.DataFrame:
    """preprocess combined description."""

    features = ["Category", "Type", "Class"]
    subsets = ["Chicago", None]

    df = permit_df.copy()
    
    df["comb_desc"] = df["EXTRACTED_DESCRIPTION"] + " " + df["PMT_DESCRP"]
    df["comb_desc_clean"] = df["comb_desc"].apply(clean_text_nltk)
    features = ["Category", "Type", "Class"]
    subsets = ["Chicago", None]
    mname = "bpsnlpmodel"

    predicted_df = []

    for feature in features:
        for subset in subsets:
            filename = f"{mname}_{feature}"
            if subset is None:
                inpath = os.path.join(PKL_MODEL_PATH, f"{filename}_fitted.pkl")
            else:
                inpath = os.path.join(PKL_MODEL_PATH, f"{filename}_{subset}_fitted.pkl")

            pred_df = apply_pkl_model(
                permit_df=df,
                pkl_path=inpath,
                infield_name="comb_desc_clean",  # Use the cleaned combined description for prediction
                outfield_name=f"predicted"  # Output field for the specific feature
            )
            pred_df = pred_df[["predicted"]]
            pred_df['feature'] = feature
            pred_df['subset'] = str(subset)


            predicted_df.append(pred_df)

    
    # Combine all predictions into a single DataFrame
    concat_df = pd.concat(predicted_df, ignore_index=False)


    return concat_df

def compare_subsets(feature, df):
    # Subset data for the feature
    print(f"Feature: {feature}")
    subset_chicago = df[(df['feature'] == feature) & (df['subset'] == 'Chicago')]['predicted']
    subset_none = df[(df['feature'] == feature) & (df['subset'] == 'None')]['predicted']
    
    # Perform statistical test (e.g., Chi-square test)
    contingency_table = pd.crosstab(subset_chicago, subset_none)
    print(contingency_table) # for debugging

    # return 

    chi2, p, dof, expected = chi2_contingency(contingency_table)
    
    # Print results
    print(f"Feature: {feature}")
    print(f"Chi-square statistic: {chi2}")
    print(f"P-value: {p}")
    if p < 0.05:
        print("Significant difference between subsets")
    else:
        print("No significant difference between subsets")
    print()

#%%
def main():
    test_df = pd.read_parquet("C:/Users/ndece/Github/bps_pipeline/.data/cm_permit_files/reveal-gc-2020-41.csv.parquet")
    test_df = test_df[["EXTRACTED_DESCRIPTION","PMT_DESCRP", "SITE_STATE", "SITE_JURIS", 'PMT_UNITS']]

    PKL_MODEL_PATH = "C:/Users/ndece/Github/bps_pipeline/pkl/"

    p = preprocess_comb_desc(test_df,PKL_MODEL_PATH)

    features = p['feature'].unique()
    for feature in features:
        compare_subsets(feature, p)


# ------------------------------------------------------------
# dagster asset definitions
# ------------------------------------------------------------

def v0_0_1(permit_df: pd.DataFrame) -> pd.DataFrame:
    """calculate permit month from permit date."""
    df = permit_df.copy()
    p = preprocess_comb_desc(df, "C:/Users/ndece/Github/bps_pipeline/pkl/")
    pivot_df = p.reset_index(names="index").pivot(index=["index", "subset"], columns=["feature"], values="predicted").reset_index().set_index("index")
    
    return pivot_df

assets = {
    "field_name": "cct",
    "active_versions": [v0_0_1]  # List of active versions for this asset
}