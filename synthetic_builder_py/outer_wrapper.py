import json

import pandas as pd

from .SyntheticBuilder import SyntheticBuilder
from .SyntheticLeg import DataType
from .utils import PATH

def extract_contracts_multipliers_operators( exp: str):
    try:
        contracts = []
        multipliers = []
        operators = []
        s = "" 

        itr  = 0
        while itr < len(exp):
            if itr == 0:
                if exp[itr] == "-":
                    operators.append(exp[itr])
                else:
                    operators.append("+")

            if exp[itr] not in "+-*0123456789":
                if(itr == 0 or exp[itr-1] in "+-"):
                    multipliers.append(1)
                else:
                    mul = ""
                    temp_itr = itr-2
                    while(temp_itr >= 0):
                        if(not("0" <= exp[temp_itr] and exp[temp_itr] <= "9")):
                            break
                        else:
                            mul += exp[temp_itr]
                        temp_itr -= 1

                    multipliers.append(int(mul[::-1]))  

                s = ""
                while itr < len(exp):
                    if exp[itr] == "+" or exp[itr] == "-":
                        operators.append(exp[itr])
                        contracts.append(s)
                        break
                    else:
                        s += exp[itr]
                    
                    itr += 1
                    if itr == len(exp):
                        contracts.append(s)
            else:
                itr += 1

        if not (len(contracts) == len(multipliers) and len(multipliers) == len(operators)):
            raise 
        
        # for contract in contracts:
        #     _ = int(contract[-2:])
        #     _ = MonthMap.month(contract[-3])

        #     sym = contract[:-3]
        #     valid_months = Ticker.SYMBOLS[sym].contract_months
        #     if valid_months.replace("-", "").find(contract[-3]) == -1:
        #         raise ValueError(f"[-] DataPipeline.extract_contracts_multipliers_operatiors \
        #                             Invalid expression contract month in {contract} fail to parse")

        # year_offset = extract_year_offset(contracts)
        # if(year_offset < 0):
        #     raise ValueError(f"[-] DataPipeline.extract_contracts_multipliers_operatiors \
        #                         Invalid expression contract year is less than current year in {contracts} fail to parse")   

        return contracts, multipliers, operators
    
    except:
        raise ValueError(f"[-] DataPipeline.extract_contracts_multipliers_operatiors Invalid expression {exp} fail to parse")


def wrapper(
    exp: str,
    back_adjustd : bool = True,
    start_year : int = 2010,
    offset: int = 10,
    max_lookback_for_back_adjust : int = 10,
) -> pd.DataFrame:
    
    exp = exp.replace(" ", "")
    contracts, multipliers, operators = extract_contracts_multipliers_operators(exp)

    with open(PATH.META_JSON_LOCAL, "r") as file:
       contract_data = json.load(file)

    smallest_expiry = None
    smallest_expiry_itr = -1
    for itr in range(len(contracts)):
        isAdded = False
        for itr_instrument in contract_data["productContract"]:
            if itr_instrument["symbol"] == contracts[itr][:-3]:
                for itr_contract in itr_instrument["contracts"]:
                    if itr_contract["contractCode"] == contracts[itr][-3:]:
                        if smallest_expiry == None:
                            smallest_expiry = pd.Timestamp(itr_contract["expiry"])
                            smallest_expiry_itr = itr
                        else:
                            if smallest_expiry > pd.Timestamp(itr_contract["expiry"]):
                                smallest_expiry = pd.Timestamp(itr_contract["expiry"])
                                smallest_expiry_itr = itr

                        isAdded = True

                    if isAdded:
                        break
            if isAdded:
                break

    # assumption i have rt_contract for init expression
    rt_contract = contracts[smallest_expiry_itr]
    # print(rt_contract)

    legs = []

    for itr in range(len(contracts)):
        leg = {}
        leg["contract"] = contracts[itr]
        leg["contract_roll_months"] = contracts[itr][-3]
        leg["rt_contract"] = rt_contract
        leg["rt_contract_roll_months"] = rt_contract[-3]
        leg["offset"] =  offset
        leg["max_lookback"] = max_lookback_for_back_adjust
        leg["multiplier"] = multipliers[itr]*(-1 if operators[itr] == '-' else 1)
        
        legs.append(leg)


    sb = SyntheticBuilder(
        legs = legs,
        data_type = DataType.backadjusted if back_adjustd else DataType.continuous,
        start_year=start_year
    )

    sb.create()
    return sb.df

if __name__ == "__main__":
    df1 = wrapper(
        exp = "ZMN25-ZMQ25-ZMV25-3*ZMZ25 + 8*ZMF26-4*ZMH26 + ZSN25-ZSQ25-ZSU25 + ZSN26 + ZLN25-4*ZLU25 + 5*ZLZ25-2*ZLH26",
        offset = 10,
        start_year=2010,
        max_lookback_for_back_adjust=10,
    )

    print(df1)

    # df2 = wrapper(
    #     exp = "ZMN25-ZMQ25-ZMV25-3*ZMZ25 + 8*ZMF26-4*ZMH26 + ZSN25-ZSQ25-ZSU25 + ZSN26 + ZLN25-4*ZLU25 + 5*ZLZ25-2*ZLH26",
    #     offset = 10,
    #     start_year=2016,
    #     max_lookback_for_back_adjust=10,
    #     back_adjustd=False,
    # )

    # print(df2)