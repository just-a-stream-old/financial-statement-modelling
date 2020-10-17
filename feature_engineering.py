import pandas as pd


class FeatureExtractor:

    def __init__(self):
        # Pass configuration parameters here
        pass

    @staticmethod
    def extract_features(financials_df_dictionary: dict) -> dict:

        features_df_dictionary = {}

        for statement_type, statement_df in financials_df_dictionary.items():
            if statement_type == "balance_sheet_df":
                features_df_dictionary[statement_type] = BalanceSheetExtractor(statement_df).extract_features()

            elif statement_type == "income_statement_df":
                features_df_dictionary[statement_type] = IncomeStatementExtractor(statement_df).extract_features()

            elif statement_type == "cash_flow_df":
                features_df_dictionary[statement_type] = CashFlowExtractor(statement_df).extract_features()

            else:
                print(f"Method: extract_features() does not recognise statement type: {statement_type}")

        return features_df_dictionary

    @staticmethod
    def extract_labels(time_series_df: pd.DataFrame, comparison_symbol: str) -> pd.DataFrame:
        spy_series = time_series_df.loc[time_series_df["symbol"] == comparison_symbol]

        return time_series_df

class BalanceSheetExtractor:

    def __init__(self, balance_sheet_df: pd.DataFrame):
        self.balance_sheet_df = balance_sheet_df

    def extract_features(self) -> pd.DataFrame:
        features_df = pd.DataFrame()
        # Add Index Columns
        features_df["symbol"] = self.balance_sheet_df["symbol"]
        features_df["date"] = self.balance_sheet_df["date"]
        features_df["period"] = self.balance_sheet_df["period"]
        # Verbatim Features From FM.v1
        features_df = self.determine_net_tangible_assets_per_total_assets(features_df)
        features_df = self.determine_true_working_capital_per_total_current_assets(features_df)
        features_df = self.determine_if_true_working_capital_is_positive(features_df)
        # Additional Features Specific To FMP

        # Other Ad Lib Features (No Research)
        features_df = self.determine_long_term_debt_per_total_debt(features_df)
        features_df = self.determine_total_investments_per_total_debt(features_df)
        return features_df

    def determine_net_tangible_assets_per_total_assets(self, features_df: pd.DataFrame) -> pd.DataFrame:
        features_df["net_tangible_assets/total_assets"] = (
                self.balance_sheet_df["totalAssets"] - self.balance_sheet_df["intangibleAssets"]) / self.balance_sheet_df["totalAssets"]
        return features_df

    def determine_true_working_capital_per_total_current_assets(self, features_df: pd.DataFrame) -> pd.DataFrame:
        features_df["true_working_capital/total_assets"] = (
                self.balance_sheet_df["totalCurrentAssets"] - self.balance_sheet_df["totalCurrentLiabilities"]) / self.balance_sheet_df["totalCurrentAssets"]
        return features_df

    def determine_if_true_working_capital_is_positive(self, features_df: pd.DataFrame) -> pd.DataFrame:
        if_positive_values = []
        for current_assets, current_liabilities in zip(self.balance_sheet_df["totalCurrentAssets"], self.balance_sheet_df["totalCurrentLiabilities"]):
            true_working_capital = current_assets - current_liabilities
            if true_working_capital > 0:
                if_positive_values.append(True)
            else:
                if_positive_values.append(False)
        features_df["positive_true_working_capital"] = if_positive_values
        return features_df

    def determine_long_term_debt_per_total_debt(self, features_df: pd.DataFrame) -> pd.DataFrame:
        features_df["long_term_debt/debt"] = self.balance_sheet_df["longTermDebt"] / self.balance_sheet_df[
            "totalDebt"]
        return features_df

    def determine_total_investments_per_total_debt(self, features_df: pd.DataFrame) -> pd.DataFrame:
        features_df["investments/debt"] = self.balance_sheet_df["totalInvestments"] / self.balance_sheet_df[
            "totalDebt"]
        return features_df


class IncomeStatementExtractor:

    def __init__(self, income_statement_df: pd.DataFrame):
        self.income_statement_df = income_statement_df

    def extract_features(self) -> pd.DataFrame:
        features_df = pd.DataFrame()
        # Add Index Columns
        features_df["symbol"] = self.income_statement_df["symbol"]
        features_df["date"] = self.income_statement_df["date"]
        features_df["period"] = self.income_statement_df["period"]
        # Verbatim Features From FM.v1
        features_df = self.determine_gross_profit_per_total_revenue(features_df)
        features_df = self.determine_r_and_d_expenses_per_total_operating_expenses(features_df)
        features_df = self.determine_general_and_admin_expenses_per_total_operating_expenses(features_df)
        features_df = self.determine_r_and_d_expenses_per_gross_profit(features_df)
        features_df = self.determine_operating_income_per_operating_expenses(features_df)
        features_df = self.determine_net_income_per_gross_profit(features_df)
        # Additional Features Specific To FMP
        features_df["gross_profit_ratio"] = self.income_statement_df["grossProfitRatio"]
        features_df["EBITDA_ratio"] = self.income_statement_df["ratioEarningsBeforeInterestTaxesDepreciationAmortisation"]
        features_df["operating_income_ratio"] = self.income_statement_df["operatingIncomeRatio"]
        features_df["income_before_tax_ratio"] = self.income_statement_df["incomeBeforeTaxRatio"]
        features_df["net_income_ratio"] = self.income_statement_df["netIncomeRatio"]
        features_df["eps_diluted"] = self.income_statement_df["EpsDiluted"]
        return features_df

    def determine_gross_profit_per_total_revenue(self, features_df: pd.DataFrame) -> pd.DataFrame:
        features_df["gross_profit/revenue"] = self.income_statement_df["netIncome"] / self.income_statement_df[
            "revenue"]
        return features_df

    def determine_r_and_d_expenses_per_total_operating_expenses(self, features_df: pd.DataFrame) -> pd.DataFrame:
        features_df["r_and_d/operating_expenses"] = self.income_statement_df["researchAndDevelopmentExpenses"] / \
                                                    self.income_statement_df[
                                                        "operatingExpenses"]
        return features_df

    def determine_general_and_admin_expenses_per_total_operating_expenses(self,
                                                                          features_df: pd.DataFrame) -> pd.DataFrame:
        features_df["general_admin/operating_expenses"] = self.income_statement_df["generalAndAdministrativeExpenses"] / \
                                                          self.income_statement_df[
                                                              "operatingExpenses"]
        return features_df

    def determine_r_and_d_expenses_per_gross_profit(self, features_df: pd.DataFrame) -> pd.DataFrame:
        features_df["r_and_d/gross_profit"] = self.income_statement_df["researchAndDevelopmentExpenses"] / \
                                              self.income_statement_df[
                                                  "grossProfit"]
        return features_df

    def determine_operating_income_per_operating_expenses(self, features_df: pd.DataFrame) -> pd.DataFrame:
        features_df["operating_income/operating_expenses"] = self.income_statement_df["operatingIncome"] / \
                                                             self.income_statement_df[
                                                                 "operatingExpenses"]
        return features_df

    def determine_net_income_per_gross_profit(self, features_df: pd.DataFrame) -> pd.DataFrame:
        features_df["net_income/gross_profit"] = self.income_statement_df["netIncome"] / self.income_statement_df[
            "grossProfit"]
        return features_df


class CashFlowExtractor:

    def __init__(self, cash_flow_df: pd.DataFrame):
        self.cash_flow_df = cash_flow_df

    def extract_features(self) -> pd.DataFrame:
        features_df = pd.DataFrame()
        # Add Index Columns
        features_df["symbol"] = self.cash_flow_df["symbol"]
        features_df["date"] = self.cash_flow_df["date"]
        features_df["period"] = self.cash_flow_df["period"]
        # Verbatim Features From FM.v1
        features_df = self.determine_net_income_per_cash_flow_from_operating_activities(features_df)
        features_df = self.determine_if_positive_cash_flow_from_operating_activities(features_df)
        features_df = self.determine_if_net_income_is_smaller_than_cash_flow_from_operating_activities(features_df)
        features_df = self.determine_if_dividends_payed_is_smaller_than_cash_flow_from_operating_activities(features_df)
        features_df = self.determine_net_borrowing_per_dividends_paid(features_df)
        features_df = self.determine_free_cash_flow_per_cash_flow_from_operating_activities(features_df)
        # Additional Features Specific To FMP
        # Other Ad Lib Features (No Research)
        features_df = self.determine_net_change_in_cash_per_start_cash(features_df)
        features_df = self.determine_effect_forex_on_cash_per_net_change_in_cash(features_df)
        return features_df

    def determine_net_income_per_cash_flow_from_operating_activities(self, features_df: pd.DataFrame) -> pd.DataFrame:
        features_df["net_income/cf_oa"] = self.cash_flow_df["netIncome"] / self.cash_flow_df[
            "netCashProvidedByOperatingActivities"]
        return features_df

    def determine_if_positive_cash_flow_from_operating_activities(self, features_df: pd.DataFrame) -> pd.DataFrame:
        if_positive_values = []
        for cash in self.cash_flow_df["netCashProvidedByOperatingActivities"]:
            if cash > 0:
                if_positive_values.append(True)
            else:
                if_positive_values.append(False)
        features_df["positive_cf_oa"] = if_positive_values
        return features_df

    def determine_if_net_income_is_smaller_than_cash_flow_from_operating_activities(self,
                                                                                    features_df: pd.DataFrame) -> pd.DataFrame:
        if_net_income_smaller_values = []
        for net_income, cash_flow in zip(self.cash_flow_df["netIncome"],
                                         self.cash_flow_df["netCashProvidedByOperatingActivities"]):
            if net_income < cash_flow:
                if_net_income_smaller_values.append(True)
            else:
                if_net_income_smaller_values.append(False)
        features_df["net_income_smaller_cf_oa"] = if_net_income_smaller_values
        return features_df

    def determine_if_dividends_payed_is_smaller_than_cash_flow_from_operating_activities(self,
                                                                                         features_df: pd.DataFrame) -> pd.DataFrame:
        if_dividends_smaller_values = []
        for dividends, cash_flow in zip(self.cash_flow_df["dividendsPaid"],
                                        self.cash_flow_df["netCashProvidedByOperatingActivities"]):
            if dividends < cash_flow:
                if_dividends_smaller_values.append(True)
            else:
                if_dividends_smaller_values.append(False)
        features_df["dividends_paid_smaller_cf_oa"] = if_dividends_smaller_values
        return features_df

    def determine_net_borrowing_per_dividends_paid(self, features_df: pd.DataFrame) -> pd.DataFrame:
        features_df["net_borrowing/dividends_paid"] = self.cash_flow_df["netCashUsedProvidedByFinancingActivities"] / \
                                                      self.cash_flow_df[
                                                          "dividendsPaid"]
        return features_df

    def determine_free_cash_flow_per_cash_flow_from_operating_activities(self,
                                                                         features_df: pd.DataFrame) -> pd.DataFrame:
        features_df["free_cf/cf_oa"] = self.cash_flow_df["freeCashFlow"] / self.cash_flow_df[
            "netCashProvidedByOperatingActivities"]
        return features_df

    def determine_net_change_in_cash_per_start_cash(self, features_df: pd.DataFrame) -> pd.DataFrame:
        features_df["net_change_cash/start_cash"] = self.cash_flow_df["netChangeInCash"] / self.cash_flow_df[
            "cashAtBeginningOfPeriod"]
        return features_df

    def determine_effect_forex_on_cash_per_net_change_in_cash(self, features_df: pd.DataFrame) -> pd.DataFrame:
        features_df["effect_forex_on_cash/net_change_cash"] = self.cash_flow_df["effectOfForexChangesOnCash"] / self.cash_flow_df[
            "netChangeInCash"]
        return features_df
