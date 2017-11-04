import pandas as pd
from similarity.Similarity import Similarity
from fuzzywuzzy import fuzz, process
import numpy as np
import json

import sys
reload(sys)
sys.setdefaultencoding('utf8')

def get_uni_rank(uni, uni_dict):
    if uni == "Unknown":
        return np.nan
    best_match = process.extractOne(uni, uni_dict.keys())
    if best_match[1] >= 85:  # similar enough
        return uni_dict[best_match[0]]


class Rank:

    def __int__(self, filename, nus, univeristy_filename):
        # filename = "./crawler/data/SAMPLE_JSON.json"
        # univeristy_filename = "./crawler/data/UNIVERSITY_LINK.json"
        # nus = {u'department': u'Geography',
        #              u'name': u'Prof Clive Agnew research profile - personal details   ',
        #              u'phd_school': u'University of East Anglia, School of Development Studies',
        #              u'phd_year': 1980,
        #              u'position': u'Professor',
        #              u'profile_link': u'http://www.manchester.ac.uk/research/Clive.agnew/',
        #              u'promotion_year': 1999,
        #              u'text_raw': u'The water balance approach to the development of rainfed agriculture in South West Niger.',
        #              u'university': u'The University of Manchester'
        # }
        self.data = pd.read_json(filename)
        self.university = pd.read_json(univeristy_filename, orient = "index")["Rank"]\
            .apply(lambda x: int(x.split("=")[-1])).to_dict()
        self.cols = self.data.columns.tolist()
        self.nus = nus
        self.data = self.data[self.data["position"] == nus["position"]]
        self.promo = False
        self.phd = False
        self.phdUni = False
        self.research = False

    def get_rank_scores(self, metrics= ["PHD YEAR", "PHD UNIVERSITY", "RESEARCH AREA SIMILARITY", "PROMO YEAR"]):
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


    def get_top_preview(self, tag, n = -1): # for preview purpose
        if n < 1:
            if tag == "peer":
                n = 6
            if tag == "aspirant":
                n = 4
            else:
                n = 5
        return self.data[self.data["tag"] == tag].sort_values("final_score", ascending = False)[self.cols].head(n)

    def export_ranked_result(self, filename = "./SAMPLE_RANKS.xlsx"):
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
        sim.add_nus_info(pd.DataFrame.from_dict([self.nus]))
        self.data = self.data.merge(sim.get_avg_score(), right_index = True, left_index = True)
        self.research = True



    
        