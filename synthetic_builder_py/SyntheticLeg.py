from enum import Enum, auto
from datetime import datetime
import json 

import pandas as pd
from pandas.tseries.offsets import BDay
import numpy as np 


from .get_outright import (
    get_outright_df
)

from .utils import (
    PATH,
    move_contract_to_given_prev_valid_month,
)

class DataType(Enum):
    continuous = auto()
    backadjusted = auto()

# how many working day before contract expire you want to roll 
# offset = 1 implies roll on last day

class SyntheticLeg:
    def __init__(
        self,
        contract : str,
        contract_roll_months : str,
        rt_contract : str, # roll_trigger_contract
        rt_contract_roll_months : str,
        offset : int,
        data_type : DataType, 
        multiplier : int,
        max_lookback : int = None,
        start_year : int = None,
        end_year : int = None,
        back_adjust_mode : int = 1,
    ):
        if (
            (data_type == DataType.backadjusted and max_lookback == None) or
            len(contract_roll_months) != len(rt_contract_roll_months)
        ):
            raise

        if start_year == None or start_year < 2010:
            self.start_year = 2010
        else :
            self.start_year = start_year

        if end_year == None or end_year > datetime.now().year:
            self.end_year = datetime.now().year
        else:
            self.end_year = end_year

        # temp arrangment 
        with open(PATH.META_JSON_LOCAL, "r") as file:
            self.contract_data = json.load(file)

        
        self.sym = contract[:-3]
        self.currency_multiplier = None
        for itr_contract in self.contract_data["productContract"]:
            if itr_contract["symbol"] == self.sym:
                self.currency_multiplier = int(itr_contract["currencyMultiplier"])

        self.rt_sym = rt_contract[:-3]
        self.rt_contract_data = None
        for itr_contract in self.contract_data["productContract"]:
            if itr_contract["symbol"] == self.rt_sym:
                self.rt_contract_data = itr_contract["contracts"]


        self.contract = contract
        self.contract_roll_months = contract_roll_months
        self.rt_contract = rt_contract
        self.rt_contract_roll_months = rt_contract_roll_months
        self.offset = offset
        self.data_type = data_type
        self.multiplier = multiplier
        self.max_lookback = max_lookback
        self.df = pd.DataFrame()
        self.back_adjust_mode = back_adjust_mode

    def create(
        self,
    ) -> None:
        if self.df.empty:
            contract= self.contract
            rt_contract =  self.rt_contract

            contract_df_list = []
            rt_contract_df_list = []
            rt_contract_list = []

            while True:
                if self.start_year%100 > int(contract[-2:]):
                    break

                try:
                    ok, contract_df = get_outright_df(
                        contract,
                        "c", # only provide close price
                    )

                    if not ok:
                        break

                    ok, rt_contract_df = get_outright_df(
                        rt_contract,
                        "c",
                    )

                    if not ok:
                        break
                
                except:
                    break

                contract_df_list.append(contract_df)
                rt_contract_df_list.append(rt_contract_df)
                rt_contract_list.append(rt_contract)
                
                contract = move_contract_to_given_prev_valid_month(
                    contract,
                    self.contract_roll_months,
                )

                rt_contract = move_contract_to_given_prev_valid_month(
                    rt_contract,
                    self.rt_contract_roll_months,
                )

            if len(contract_df_list) != len(rt_contract_df_list):
                raise


            # only Rollmethod calender 
            num_of_contract = len(contract_df_list)
            rolled_df_list = []


            for itr in range(num_of_contract):

                isFound = None
                for itr_contract in self.rt_contract_data:
                    if itr_contract["contractCode"] == rt_contract_list[itr][-3:]:
                        isFound = itr_contract

                if isFound == None:
                    day_df = rt_contract_df_list[itr].index.normalize().unique()

                    if len(day_df) < self.offset:
                        raise

                    roll_date = day_df[-self.offset]   
                else:
                    roll_date = pd.Timestamp(isFound["expiry"]) - BDay(self.offset)

                rolled_df = contract_df_list[itr].loc[:roll_date].copy()
                rolled_df["roll_date"] = roll_date

                rolled_df_list.append(rolled_df)

            # isFirstBackadjusteSucceed = False
            for itr in range(1, num_of_contract+1):
                if self.df.empty:
                    self.df = rolled_df_list[-itr].copy()

                else:
                    temp = rolled_df_list[-itr]
                    trimmed = temp[temp.index > self.df.index[-1]]

                    if self.data_type == DataType.backadjusted:
                        if self.back_adjust_mode == 1:
                            lookback_itr = 1
                            diff = None
                            
                            # this should return message like can't backadjust 
                            if len(self.df.index) < self.max_lookback:
                                lookback_itr = self.max_lookback + 1

                            while self.max_lookback >= lookback_itr:
                                diff_date = self.df.index[-lookback_itr]

                                if (
                                    diff_date in rolled_df_list[-itr].index and
                                    not pd.isna(self.df.loc[diff_date].close) and
                                    not pd.isna(rolled_df_list[-itr].loc[diff_date].close)
                                ):
                                    diff = rolled_df_list[-itr].loc[diff_date].close
                                    diff -= self.df.loc[diff_date].close
                                    break

                                lookback_itr += 1


                            if self.max_lookback < lookback_itr:
                                raise Exception(f"Fail to backadjust at {rt_contract_list[-itr]}\n {lookback_itr=}\n {self.rt_contract=}")
                                # break # from this onwards we can't backadjust the data 
                                self.df = pd.DataFrame()
                                continue     

                            # isFirstBackadjusteSucceed = True
                            # self.df = self.df + diff
                            self.df["close"] = self.df["close"] + diff
                        
                        elif self.back_adjust_mode == 2:
                            diff = trimmed.iloc[0].close
                            diff -= self.df.iloc[-1].close
                            self.df["close"] = self.df["close"] + diff
                        else:
                            raise Exception(f"Invalid backadjust mode")

                    self.df = pd.concat([self.df, trimmed])

            self.df = self.df.loc[(self.df.iloc[0].roll_date - pd.DateOffset(years=1)):]
            self.df.close *= self.currency_multiplier
            self.df.close *= self.multiplier

if __name__ == "__main__":

    leg1 = SyntheticLeg(
        "ZMF27",
        "F",
        "ZSX25",
        "X",
        10,
        DataType.backadjusted,
        1,
        max_lookback=10,
        start_year=2018,
    )

    leg1.create()
    print(leg1.df)


    # leg2 = SyntheticLeg(
    #     "ZMH26",
    #     "H",
    #     "ZMN25",
    #     "N",
    #     40,
    #     DataType.continuous,
    #     1,
    #     max_lookback=10,
    #     start_year=2016,
    # )

    # leg2.create()
    # print(leg2.df)