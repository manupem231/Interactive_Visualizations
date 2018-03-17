# import necessary libraries
import pandas as pd
import numpy as np
import csv
import os

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, desc, select

from flask import (Flask, render_template, jsonify)

# engine = create_engine("sqlite:///DataSets/belly_button_biodiversity.sqlite")

# # reflect an existing database into a new model
# Base = automap_base()
# # reflect the tables
# Base.prepare(engine, reflect=True)

# # Save reference to the table
# OTU = Base.classes.otu
# Samples = Base.classes.samples
# Samples_Metadata= Base.classes.samples_metadata

# # Create our session (link) from Python to the DB
# session = Session(engine)

#################################################
# Reading Data from CSV
#################################################

file_biodiversity_samples = os.path.join('DataSets/belly_button_biodiversity_samples.csv')
file_biodiversity_otu = os.path.join('DataSets/belly_button_biodiversity_otu_id.csv')
file_biodiversity_metadata = os.path.join('DataSets/Belly_Button_Biodiversity_Metadata.csv')
file_biodiversity_metadata_columns = os.path.join('DataSets/metadata_columns.csv')

biodiversity_samples_df = pd.read_csv(file_biodiversity_samples)
biodiversity_otu_df = pd.read_csv(file_biodiversity_otu)
biodiversity_metadata_df = pd.read_csv(file_biodiversity_metadata)
biodiversity_columns_df = pd.read_csv(file_biodiversity_metadata_columns)

#################################################
# Flask Setup
#################################################

app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def home():
    """Render Home Page."""
    return render_template("index.html")


@app.route("/names")
def samples_data():
    """Return Belly Button Biodiversity Sample Names"""

    # query for list of sample names
    result_names = list(biodiversity_samples_df)
    result_names.remove('otu_id')
    return jsonify(result_names)


@app.route("/otu")
def otu_data():
    """Return Biodiversity otu descriptions"""

    # query for list of otu descriptions
    result_otu = list(biodiversity_otu_df['lowest_taxonomic_unit_found'])
    return jsonify(result_otu)


@app.route("/metadata/<sample>")
def sample_metadata(sample):
    """Return Biodiversity metadata sample"""

    biodiversity_meta_updated_df = biodiversity_metadata_df[['AGE','BBTYPE', 'ETHNICITY', 'GENDER', 'LOCATION', 'SAMPLEID']]
    biodiversity_metadata_updated_df = biodiversity_meta_updated_df[biodiversity_meta_updated_df['SAMPLEID'] == int(sample[3:])] # sample[3:] returns "940" from 'BB_940'
    biodiversity_metadata_df_to_dict = biodiversity_metadata_updated_df.to_dict('records')
    return jsonify(biodiversity_metadata_df_to_dict[0])

@app.route("/wfreq/<sample>")
def washing_frequency(sample):
    """Weekly Washing Frequency as a number"""

    washing_frequency_df = biodiversity_metadata_df[['SAMPLEID', 'WFREQ']]
    washing_frequency_updated_df = washing_frequency_df[washing_frequency_df['SAMPLEID'] == int(sample[3:])]
    result_washing_frequency = list(washing_frequency_updated_df['WFREQ'])
    result_wfreq = int(result_washing_frequency[0])
    return jsonify(result_wfreq)


@app.route("/samples/<sample>")
def OTU_ID_and_Sample(sample):
    """OTU IDs and Sample Values for a given sample."""

    sample_df = biodiversity_samples_df[(biodiversity_samples_df[sample] > 0.0)]
    sample_sort_df = sample_df.sort_values(by = sample, ascending=False)

    sample_data = [{
        "otu_ids": sample_sort_df[sample].index.values.tolist(),
        "sample_values": sample_sort_df[sample].values.tolist()
    }]
    sample_data_df = pd.DataFrame(sample_data)
    final_sample = sample_data_df.to_json(orient = 'records')
    return final_sample

if __name__ == '__main__':
    app.run(debug=True)
