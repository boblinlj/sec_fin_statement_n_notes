import yfinance as yf
import pandas as pd
import numpy as  np
import datetime
from enter_to_database import DatabaseManagement
pd.set_option('display.max_columns',500)
pd.set_option('display.max_rows',500)
PROXY = "socks5://10.0.0.216:9050"

KEEP_COLUMNS  = ['AccountsPayable',
            'AccountsReceivable',
            'AccumulatedDepreciation',
            'AdditionalPaidInCapital',
            'AllowanceForDoubtfulAccountsReceivable',
            'Amortization',
            'AmortizationCashFlow',
            'AmortizationOfIntangibles',
            'AmortizationOfIntangiblesIncomeStatement',
            'AssetImpairmentCharge',
            'AssetsHeldForSaleCurrent',
            'AvailableForSaleSecurities',
            'AverageDilutionEarnings',
            'BasicAverageShares',
            'BasicEPS',
            'BeginningCashPosition',
            'BuildingsAndImprovements',
            'CapitalExpenditure',
            'CapitalExpenditureReported',
            'CapitalLeaseObligations',
            'CapitalStock',
            'CashAndCashEquivalents',
            'CashCashEquivalentsAndShortTermInvestments',
            'CashDividendsPaid',
            'CashEquivalents',
            'CashFinancial',
            'CashFlowFromContinuingFinancingActivities',
            'CashFlowFromContinuingInvestingActivities',
            'CashFlowFromContinuingOperatingActivities',
            'CashFromDiscontinuedFinancingActivities',
            'CashFromDiscontinuedInvestingActivities',
            'CashFromDiscontinuedOperatingActivities',
            'ChangeInAccountPayable',
            'ChangeInAccruedExpense',
            'ChangeInIncomeTaxPayable',
            'ChangeInInventory',
            'ChangeInOtherCurrentAssets',
            'ChangeInOtherCurrentLiabilities',
            'ChangeInOtherWorkingCapital',
            'ChangeInPayable',
            'ChangeInPayablesAndAccruedExpense',
            'ChangeInPrepaidAssets',
            'ChangeInReceivables',
            'ChangeInTaxPayable',
            'ChangeInWorkingCapital',
            'ChangesInAccountReceivables',
            'ChangesInCash',
            'CommercialPaper',
            'CommonStock',
            'CommonStockDividendPaid',
            'CommonStockEquity',
            'CommonStockIssuance',
            'CommonStockPayments',
            'ConstructionInProgress',
            'CostOfRevenue',
            'CurrentAccruedExpenses',
            'CurrentAssets',
            'CurrentCapitalLeaseObligation',
            'CurrentDebt',
            'CurrentDebtAndCapitalLeaseObligation',
            'CurrentDeferredLiabilities',
            'CurrentDeferredRevenue',
            'CurrentLiabilities',
            'CurrentNotesPayable',
            'CurrentProvisions',
            'DeferredIncomeTax',
            'DeferredTax',
            'DefinedPensionBenefit',
            'Depreciation',
            'DepreciationAmortizationDepletion',
            'DepreciationAmortizationDepletionIncomeStatement',
            'DepreciationAndAmortization',
            'DepreciationAndAmortizationInIncomeStatement',
            'DerivativeProductLiabilities',
            'DilutedAverageShares',
            'DilutedEPS',
            'DilutedNIAvailtoComStockholders',
            'DividendReceivedCFO',
            'DividendsPayable',
            'DividendsReceivedCFI',
            'DuetoRelatedPartiesCurrent',
            'EarningsFromEquityInterest',
            'EarningsLossesFromEquityInvestments',
            'EBIT',
            'EffectOfExchangeRateChanges',
            'EmployeeBenefits',
            'EndCashPosition',
            'FinancingCashFlow',
            'FinishedGoods',
            'ForeignCurrencyTranslationAdjustments',
            'FreeCashFlow',
            'GainLossOnInvestmentSecurities',
            'GainLossOnSaleOfBusiness',
            'GainLossOnSaleOfPPE',
            'GainOnSaleOfBusiness',
            'GainOnSaleOfPPE',
            'GainOnSaleOfSecurity',
            'GainsLossesNotAffectingRetainedEarnings',
            'GeneralAndAdministrativeExpense',
            'Goodwill',
            'GoodwillAndOtherIntangibleAssets',
            'GrossAccountsReceivable',
            'GrossPPE',
            'GrossProfit',
            'ImpairmentOfCapitalAssets',
            'IncomeTaxPaidSupplementalData',
            'IncomeTaxPayable',
            'InsuranceAndClaims',
            'InterestExpense',
            'InterestExpenseNonOperating',
            'InterestIncome',
            'InterestIncomeNonOperating',
            'InterestPaidSupplementalData',
            'InterestPayable',
            'Inventory',
            'InvestedCapital',
            'InvestingCashFlow',
            'InvestmentinFinancialAssets',
            'InvestmentsAndAdvances',
            'InvestmentsinAssociatesatCost',
            'InvestmentsInOtherVenturesUnderEquityMethod',
            'IssuanceOfCapitalStock',
            'IssuanceOfDebt',
            'LandAndImprovements',
            'Leases',
            'LineOfCredit',
            'LongTermCapitalLeaseObligation',
            'LongTermDebt',
            'LongTermDebtAndCapitalLeaseObligation',
            'LongTermDebtIssuance',
            'LongTermDebtPayments',
            'LongTermEquityInvestment',
            'LongTermProvisions',
            'MachineryFurnitureEquipment',
            'MinimumPensionLiabilities',
            'MinorityInterest',
            'MinorityInterests',
            'NetBusinessPurchaseAndSale',
            'NetCommonStockIssuance',
            'NetDebt',
            'NetForeignCurrencyExchangeGainLoss',
            'NetIncome',
            'NetIncomeCommonStockholders',
            'NetIncomeContinuousOperations',
            'NetIncomeDiscontinuousOperations',
            'NetIncomeFromContinuingAndDiscontinuedOperation',
            'NetIncomeFromContinuingOperationNetMinorityInterest',
            'NetIncomeFromContinuingOperations',
            'NetIncomeIncludingNoncontrollingInterests',
            'NetIntangiblesPurchaseAndSale',
            'NetInterestIncome',
            'NetInvestmentPurchaseAndSale',
            'NetIssuancePaymentsOfDebt',
            'NetLongTermDebtIssuance',
            'NetNonOperatingInterestIncomeExpense',
            'NetOtherFinancingCharges',
            'NetOtherInvestingChanges',
            'NetPPE',
            'NetPPEPurchaseAndSale',
            'NetPreferredStockIssuance',
            'NetShortTermDebtIssuance',
            'NetTangibleAssets',
            'NonCurrentAccruedExpenses',
            'NonCurrentDeferredAssets',
            'NonCurrentDeferredLiabilities',
            'NonCurrentDeferredRevenue',
            'NonCurrentDeferredTaxesLiabilities',
            'NonCurrentPensionAndOtherPostretirementBenefitPlans',
            'NormalizedEBITDA',
            'NormalizedIncome',
            'OperatingCashFlow',
            'OperatingExpense',
            'OperatingGainsLosses',
            'OperatingIncome',
            'OperatingRevenue',
            'OrdinarySharesNumber',
            'OtherCashAdjustmentOutsideChangeinCash',
            'OtherCurrentAssets',
            'OtherCurrentBorrowings',
            'OtherCurrentLiabilities',
            'OtherEquityAdjustments',
            'OtherEquityInterest',
            'OtherGandA',
            'OtherIncomeExpense',
            'OtherIntangibleAssets',
            'OtherInventories',
            'OtherInvestments',
            'OtherNonCashItems',
            'OtherNonCurrentAssets',
            'OtherNonCurrentLiabilities',
            'OtherNonOperatingIncomeExpenses',
            'OtherOperatingExpenses',
            'OtherPayable',
            'OtherProperties',
            'OtherReceivables',
            'OtherShortTermInvestments',
            'OtherSpecialCharges',
            'OtherunderPreferredStockDividend',
            'Payables',
            'PayablesAndAccruedExpenses',
            'PensionAndEmployeeBenefitExpense',
            'PensionandOtherPostRetirementBenefitPlansCurrent',
            'PreferredSharesNumber',
            'PreferredStock',
            'PreferredStockDividends',
            'PreferredStockEquity',
            'PreferredStockIssuance',
            'PreferredStockPayments',
            'PrepaidAssets',
            'PretaxIncome',
            'ProceedsFromStockOptionExercised',
            'Properties',
            'ProvisionandWriteOffofAssets',
            'PurchaseOfBusiness',
            'PurchaseOfIntangibles',
            'PurchaseOfInvestment',
            'PurchaseOfPPE',
            'RawMaterials',
            'Receivables',
            'ReceivablesAdjustmentsAllowances',
            'ReconciledCostOfRevenue',
            'ReconciledDepreciation',
            'RentExpenseSupplemental',
            'RepaymentOfDebt',
            'RepurchaseOfCapitalStock',
            'ResearchAndDevelopment',
            'RestrictedCash',
            'RestructuringAndMergernAcquisition',
            'RetainedEarnings',
            'SalariesAndWages',
            'SaleOfBusiness',
            'SaleOfIntangibles',
            'SaleOfInvestment',
            'SaleOfPPE',
            'SellingAndMarketingExpense',
            'SellingGeneralAndAdministration',
            'ShareIssued',
            'ShortTermDebtIssuance',
            'ShortTermDebtPayments',
            'SpecialIncomeCharges',
            'StockBasedCompensation',
            'StockholdersEquity',
            'TangibleBookValue',
            'TaxEffectOfUnusualItems',
            'TaxProvision',
            'TaxRateForCalcs',
            'TotalAssets',
            'TotalCapitalization',
            'TotalDebt',
            'TotalEquityGrossMinorityInterest',
            'TotalExpenses',
            'TotalLiabilitiesNetMinorityInterest',
            'TotalNonCurrentAssets',
            'TotalNonCurrentLiabilitiesNetMinorityInterest',
            'TotalOperatingIncomeAsReported',
            'TotalOtherFinanceCost',
            'TotalRevenue',
            'TotalTaxPayable',
            'TotalUnusualItems',
            'TotalUnusualItemsExcludingGoodwill',
            'TradeandOtherPayablesNonCurrent',
            'TradingSecurities',
            'TreasurySharesNumber',
            'TreasuryStock',
            'UnrealizedGainLoss',
            'UnrealizedGainLossOnInvestmentSecurities',
            'WorkingCapital',
            'WorkInProcess',
            'WriteOff']

class get_stock_financial():
    
    def __init__(self, stock, update_dt = None) -> None:
        self.stock = stock
        self.updated_dt = update_dt
        self.output_df = pd.DataFrame()
    
    def _get_income_statements(self, type = 'quarterly') -> pd.DataFrame:
        is_dic = yf.Ticker(self.stock).get_income_stmt(proxy=PROXY, as_dict=True, freq=type)
        is_df = pd.DataFrame.from_dict(is_dic, orient='index')
        return is_df

    def _get_balance_sheets(self, type = 'quarterly') -> pd.DataFrame:
        bs_dic = yf.Ticker(self.stock).get_balance_sheet(proxy=PROXY, as_dict=True, freq=type)
        bs_df = pd.DataFrame.from_dict(bs_dic, orient='index')
        return bs_df

    def _get_cashflow_statements(self, type = 'quarterly') -> pd.DataFrame:
        cs_dic = yf.Ticker(self.stock).get_cash_flow(proxy=PROXY, as_dict=True, freq=type)
        cs_df = pd.DataFrame.from_dict(cs_dic, orient='index')
        return cs_df
    
    def _get_all_financial_information(self, type = 'quarterly')-> pd.DataFrame:
        return pd.concat([self._get_balance_sheets(type)
                            ,self._get_income_statements(type)
                            ,self._get_cashflow_statements(type)
                            ]
                        ,axis=1
                         )

    def _validate_data(self, df) -> None:
        
        for e in KEEP_COLUMNS:
            if e in df.columns:
                self.output_df = pd.concat([self.output_df, df[e]], axis=1)
            else:
                self.output_df[e] = np.nan

        
        self.output_df['updated_dt'] = self.updated_dt
        self.output_df.reset_index(inplace = True)
        self.output_df.rename(columns={'index':'AsofDate'}, inplace=True)
        self.output_df['ticker'] = self.stock
    
    def parse(self):
        self._validate_data(self._get_all_financial_information())
        return self.output_df
            
    def insert_to_db(self):
        self._validate_data(self._get_all_financial_information())
        DatabaseManagement(dataframe=self.output_df
                           , target_table='yahoo_quarterly_financial_statements'
                           , insert_index=False).insert_dataframe_to_table()
        
    
if __name__ == '__main__':
    call = get_stock_financial('WMT', '9999-12-31')
    call.insert_to_db()