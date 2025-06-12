from concurrent.futures import ThreadPoolExecutor

import pandas as pd 

from .SyntheticLeg import (
    DataType,
    SyntheticLeg,
)

class SyntheticBuilder:
    def __init__(
        self,
        legs : list,
        data_type : DataType,
        start_year : int = None,
        back_adjust_mode : int = 1,
    ):
        self.legs = legs
        self.data_type = data_type
        self.start_year = start_year
        self.leg_list = []
        self.back_adjust_mode  = back_adjust_mode

        self.df = pd.DataFrame()

    def create(self):
        for leg in self.legs:
            # print(leg)
            sleg = SyntheticLeg(
                contract  = leg["contract"],
                contract_roll_months = leg["contract_roll_months"],
                rt_contract = leg["rt_contract"],
                rt_contract_roll_months = leg["rt_contract_roll_months"],
                offset = leg["offset"],
                data_type = self.data_type, 
                multiplier = leg["multiplier"],
                max_lookback = leg["max_lookback"],
                start_year = self.start_year,
                back_adjust_mode = self.back_adjust_mode,
            )

            # sleg.create()
            self.leg_list.append(sleg)

        with ThreadPoolExecutor(max_workers=8) as executor:
            # Submit multiple tasks to the thread pool

            futures = [executor.submit(sleg.create) for sleg in self.leg_list]

            # Process the results as they complete
            for future in futures:
                result = future.result()    


        self.build()

    def build(self):
        for leg in self.leg_list:
            if self.df.empty:
                self.df = leg.df.close.copy()
            else:
                self.df += leg.df.close

        if not self.df.empty:
            # this roll date hack only give correct infromation if 
            # all the leg are rolled using same roll trigger config
            # pass
            self.df = pd.concat([self.df, self.leg_list[0].df["roll_date"]], axis=1)
            self.df = self.df.dropna()
            
            # temp sol
            # cropping using start_year some time lead to first entry wrong
            self.df = self.df.iloc[1:]
            self.df["days_to_roll"] = self.df["roll_date"] - self.df.index


if __name__ == "__main__":
    leg1 = {}
    leg1["contract"] = "CLZ24"
    leg1["contract_roll_months"] = "Z"
    leg1["rt_contract"] = "CLZ24"
    leg1["rt_contract_roll_months"] = "Z"
    leg1["offset"] =  40
    leg1["max_lookback"] = 10
    leg1["multiplier"] = 1

    leg2 = {}
    leg2["contract"] = "CLZ25"
    leg2["contract_roll_months"] = "Z"
    leg2["rt_contract"] = "CLZ24"
    leg2["rt_contract_roll_months"] = "Z"
    leg2["offset"] =  40
    leg2["max_lookback"] = 10
    leg2["multiplier"] = -1

    legs = []
    legs.append(leg1)
    legs.append(leg2)

    sb = SyntheticBuilder(
        legs,
        DataType.backadjusted,
    )

    sb.create()

    print(sb.df)