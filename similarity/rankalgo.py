import pandas as pd
from similarity.similarity import Similarity
from fuzzywuzzy import process
import numpy as np
from datetime import datetime
import sqlite3

import sys
reload(sys)
sys.setdefaultencoding('utf8')

def get_uni_rank(uni, uni_dict):
    if uni == "Unknown":
        return np.nan
    best_match = process.extractOne(uni, uni_dict.keys())
    if best_match[1] >= 85:  # similar enough
        return uni_dict[best_match[0]]

#DATA_FILENAME = "./crawler/data/SAMPLE_JSON.json"
UNI_FILENAME = "./crawler/data/UNIVERSITY_LINK.json"


class Rank:

    def __init__(self, nus):
        # nus = {u'department': u'Geography',
        #              u'name': u'Prof Clive Agnew research profile - personal details   ',
        #              u'phd_school': u'University of East Anglia, School of Development Studies',
        #              u'phd_year': 1980,
        #              u'position': u'Professor',
        #              u'profile_link': u'',
        #              u'promotion_year': 1999,
        #              u'text_raw': u'The water balance approach to the development of rainfed agriculture in South West Niger.',
        #              u'university': u'National university of Singapore'
        # }
        # metrics = ["PHD YEAR", "PHD UNIVERSITY", "RESEARCH AREA SIMILARITY", "PROMO YEAR"]
        #self.data = pd.read_json(DATA_FILENAME)
        con = sqlite3.connect("./integrated/database.db")
        self.data = pd.read_sql("select * from profiles", con)
        self.university = pd.read_json(UNI_FILENAME, orient = "index")["Rank"].apply(lambda x: int(x.split("=")[-1])).to_dict()
        self.cols = self.data.columns.tolist()
        self.nus = nus
        self.data = self.data[(self.data["department"] == nus["department"]) & (self.data["position"] == nus["position"])]
        self.promo = False
        self.phd = False
        self.phdUni = False
        self.research = False

    def get_rank_scores(self, metrics = ["PHD YEAR", "PHD UNIVERSITY", "RESEARCH AREA SIMILARITY", "PROMO YEAR"]):
        scores = []
        for l in metrics:
            if l == "PHD YEAR":
                scores.append("phd_year_score")
                if self.phd == False:
                    self.get_phd_year_score()
            if l == "PHD UNIVERSITY":
                scores.append("phd_school_score")
                if self.phdUni == False:
                    self.get_phd_uni_score()
            if l == "PROMO YEAR":
                scores.append("promotion_year_score")
                if self.promo == False:
                    self.get_promo_year_score()
            if l ==  "RESEARCH AREA SIMILARITY":
                scores.append("research_area_score")
                if self.research == False:
                    self.get_research_sim_score()
                self.cols.append("keywords")
        self.data["final_score"] = self.data[scores].mean(axis = 1)
        self.cols.append("final_score")


    def get_top_preview(self): # for preview purpose
       peer = self.data[self.data["tag"] == "peer"].sort_values("final_score", ascending=False)[self.cols].head(6)
       asp = self.data[self.data["tag"] == "aspirant"].sort_values("final_score", ascending=False)[self.cols].head(4)
       return [peer, asp]

    def export_ranked_result(self, filename = ""):
        if filename == "":
            filename = "./Results/" + self.nus["name"].strip().replace(" ", "_") + " " + datetime.now().date().strftime("%Y-%m-%d") +".xlsx"
        writer = pd.ExcelWriter(filename)
        self.data[self.data["tag"] == "peer"].sort_values("final_score", ascending = False)[self.cols].to_excel(writer, sheet_name= "PEER", index = False)
        self.data[self.data["tag"] == "aspirant"].sort_values("final_score", ascending = False)[self.cols].to_excel(writer, sheet_name= "ASPIRANT", index = False)
        writer.close()

    def get_phd_year_score(self):
        self.data["phd_year_diff"] = self.data["phd_year"].apply(lambda x: abs(int(x) - self.nus["phd_year"]) if x != "Unknown" else np.nan)
        max_diff = self.data["phd_year_diff"].max()
        self.data["phd_year_score"] = self.data["phd_year_diff"].apply(lambda x: 1 - x / max_diff)
        self.phd = True

    def get_promo_year_score(self):
        self.data["promotion_year_diff"] = self.data["promotion_year"].apply(lambda x: abs(int(x) - self.nus["promotion_year"]) if x != "Unknown" else np.nan)
        max_diff = self.data["promotion_year_diff"].max()
        self.data["promotion_year_score"] = self.data["promotion_year_diff"].apply(lambda x: 1 - x / max_diff)
        self.promo = True

    def get_phd_uni_score(self):
        nus_rank = get_uni_rank(self.nus["phd_school"], self.university)
        self.data["phd_school_rank"] = self.data["phd_school"].apply(lambda x: get_uni_rank(x, self.university))
        self.data["phd_school_rank_diff"] = np.where(self.data["phd_school_rank"] != -1,
                                                      abs(self.data["phd_school_rank"] - nus_rank),
                                                      np.nan)
        max_diff = self.data["phd_school_rank_diff"].max()
        self.data["phd_school_score"] = self.data["phd_school_rank_diff"].apply(lambda x: 1 - x / max_diff)
        self.phdUni = True

    def get_research_sim_score(self):
        sim = Similarity(self.data)
        nus_df = pd.DataFrame.from_dict([self.nus])
        sim.add_nus_info(nus_df)
        self.data = sim.get_avg_score()
        self.research = True



def run_benchmarker(nus, metrics):
    rank = Rank(nus)
    rank.get_rank_scores(metrics)
    rank.export_ranked_result()
    return rank.get_top_preview()# [peer_dataframe, aspirant_df]




        
