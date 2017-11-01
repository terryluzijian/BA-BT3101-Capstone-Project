import pandas as pd
import Similarity
from fuzzywuzzy import fuzz, process
import numpy as np

def get_uni_rank(uni, uni_ls):
    best_match = process.extractOne(uni, uni_ls)
    if best_match[1] >= 85:  # similar enough
        return uni_ls.indexof(best_match[0])


class Rank:

    def __int__(self, filename, nus, univeristy_ls):
        self.data = pd.read_json(filename)
        self.university = univeristy_ls
        self.nus = nus
        self.promo = False
        self.phd = False
        self.phdUni = False
        self.research = False

    def get_rank_scores(self, ls):
        scores = []
        for l in ls:
            if l == "PHD YEAR":
                scores.append("phd year score")
                if self.phd == False:
                    self.get_phd_year_score()
            if l == "PHD UNIVERSITY":
                scores.append("phd university score")
                if self.phdUni == False:
                    self.get_phd_uni_score()
            if l == "PROMO YEAR":
                scores.append("promo year score")
                if self.promo == False:
                    self.get_promo_year_score()
            if l ==  "RESEARCH AREA SIMILARITY":
                scores.append("research score")
                if self.research == False:
                    self.get_research_sim_score()
        self.data["final score"] = self.data[scores].mean(0)

    def get_top(self, uni, n=5):
        return self.data[self.data["uni"] == uni].sort_values("final score").head(n)

    def get_phd_year_score(self):
        self.data["phd year diff"] = self.data["phd year"].apply(lambda x: abs(x - self.nus["phd year"]))
        mean_diff = self.data["phd year diff"].mean()
        max_diff = self.data["phd year diff"].max()
        self.data["phd year score"] = self.data["phd year diff"].apply(lambda x: 1- (x - mean_diff) / max_diff)
        self.phd = True

    def get_promo_year_score(self):
        self.data["promo year diff"] = self.data["promo year"].apply(lambda x: abs(x - self.nus["promo year"]))
        mean_diff = self.data["promo year diff"].mean()
        max_diff = self.data["promo year diff"].max()
        self.data["promo year score"] = self.data["promo year diff"].apply(lambda x: 1- (x - mean_diff) / max_diff)
        self.promo = True

    def get_phd_uni_score(self):
        nus_rank = get_uni_rank(self.nus["phd univeristy"], self.university)
        self.data["phd univeristy rank"] = self.data["phd univeristy"].apply(lambda x: get_uni_rank(x, self.university) - nus_rank)
        self.data["phd univeristy rank diff"] = np.where(self.data["phd univeristy rank"] != -1, 
                                                      abs(self.data["phd univeristy rank"] - nus_rank), 
                                                      np.nan)
        mean_diff = self.data["phd univeristy rank diff"].mean()
        max_diff = self.data["phd univeristy rank diff"].max()
        self.data["phd univeristy score"] = self.data["phd univeristy rank diff"].apply(lambda x: 1- (x - mean_diff) / max_diff)
        self.data["phd univeristy score"].fillna(0)
        self.phdUni = True

    def get_research_sim_score(self):
        sim = Similarity(self.data)
        sim.add_nus_info(self.nus)
        col = sim.get_avg_score()
        self.data["research score"] = sim.get_data()[col]
        self.research = True



    
        
