import pandas as pd
import similarity.Similarity
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
        self_data = pd.read_json(filename)
        self_university = pd.read_json(univeristy_filename, orient = "index")["Rank"]\
            .apply(lambda x: int(x.split("=")[-1])).to_dict()
        self_nus = nus
        self_data = self_data[self_data["position"] == nus["position"]]
        self_promo = False
        self_phd = False
        self_phdUni = False
        self_research = False

    def get_rank_scores(self, ls= ["PHD YEAR", "PHD UNIVERSITY", "RESEARCH AREA SIMILARITY", "PROMO YEAR"]):
        scores = []
        for l in ls:
            if l == "PHD YEAR":
                scores.append("phd_year_score")
                if self_phd == False:
                    self_get_phd_year_score()
            if l == "PHD UNIVERSITY":
                scores.append("phd_school_score")
                if self_phdUni == False:
                    self_get_phd_uni_score()
            if l == "PROMO YEAR":
                scores.append("promotion_year_score")
                if self_promo == False:
                    self_get_promo_year_score()
            if l ==  "RESEARCH AREA SIMILARITY":
                scores.append("research_area_score")
                if self_research == False:
                    self_get_research_sim_score()
        self_data["final_score"] = self_data[scores].mean(axis = 1)

    def get_top(self, tag, n=5): # for preview purpose
        return self_data[self_data["tag"] == tag].sort_values("final_score", ascending = False).head(n)

    def get_phd_year_score(self):
        self_data["phd_year_diff"] = self_data["phd_year"].apply(lambda x: abs(int(x) - self_nus["phd_year"]) if x != "Unknown" else np.nan)
        max_diff = self_data["phd_year_diff"].max()
        self_data["phd_year_score"] = self_data["phd_year_diff"].apply(lambda x: 1 - x / max_diff)
        self_phd = True

    def get_promo_year_score(self):
        self_data["promotion_year_diff"] = self_data["promotion_year"].apply(lambda x: abs(int(x) - self_nus["promotion_year"]) if x != "Unknown" else np.nan)
        max_diff = self_data["promotion_year_diff"].max()
        self_data["promotion_year_score"] = self_data["promotion_year_diff"].apply(lambda x: 1 - x / max_diff)
        self_promo = True

    def get_phd_uni_score(self):
        nus_rank = get_uni_rank(self_nus["phd_school"], self_university)
        self_data["phd_school_rank"] = self_data["phd_school"].apply(lambda x: get_uni_rank(x, self_university))
        self_data["phd_school_rank_diff"] = np.where(self_data["phd_school_rank"] != -1,
                                                      abs(self_data["phd_school_rank"] - nus_rank),
                                                      np.nan)
        max_diff = self_data["phd_school_rank_diff"].max()
        self_data["phd_school_score"] = self_data["phd_school_rank_diff"].apply(lambda x: 1 - x / max_diff)
        self_phdUni = True

    def get_research_sim_score(self):
        sim = Similarity.Similarity(self_data)
        sim.add_nus_info(pd.DataFrame.from_dict([self_nus]))
        self_data = self_data.merge(sim.get_avg_score(), right_index = True, left_index = True)
        self_research = True



    
        
