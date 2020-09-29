import pandas as pd
import pprint
from repository import MongoRepository

if __name__ == '__main__':
    database = MongoRepository("nonprod")

    company_balance_sheets = database.find_one("balance_sheets", {}).get("balanceSheets")

    df = pd.DataFrame(list(company_balance_sheets))

    pprint.pprint(df.head())

    # df_list = [pd.DataFrame.from_dict(balance_sheet, orient="index") for balance_sheet in company_balance_sheets]
    #
    # pprint.pprint(df_list[0])






