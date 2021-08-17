package org.demo3.rbl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

public class testingfinal135 {

	public static void main(String[] args) {
			
			SparkSession spark = SparkSession
					  .builder().master("local[*]")
					  .appName("RBL Updates")
					  .getOrCreate();

			Dataset<Row> adf_client = spark.read()
					   .format("com.databricks.spark.csv")
					   .option("delimiter", "|")
					   .option("header", "true")
					   .load("resources/outDF.csv");
		
			System.out.println(" No of rows in adf_client= " + adf_client.count() + "  & no of colums in adf_client  = " + adf_client.columns().length);
			//from dataset-selecting columns and renaming them

	// Qry C & D
			Dataset<Row> adf_cdr_am_cd = spark.read()
					   .format("com.databricks.spark.csv")
					   .option("delimiter", "|")
					   .option("header", "true")
					   .load("resources/outDF.csv")
					   .select("ACCOUNT_NO","INITIAL_FUNDING","LAST_CREDIT_TRAN_DATE",
			        		   "LAST_DEBIT_TRAN_DATE","IB_LAST_TRAN_DATE","ATM_LAST_TRAN_DATE","OPENING_TRAN_DATE",
			        		   "LR_FREQ_TYPE","EMIAMT")
						.withColumnRenamed("ACCOUNT_NO", "B_ACCOUNT_NO")
						.withColumnRenamed("INITIAL_FUNDING", "B_INITIAL_FUNDING")
						.withColumnRenamed("LAST_CREDIT_TRAN_DATE", "B_LAST_CREDIT_TRAN_DATE")
						.withColumnRenamed("LAST_DEBIT_TRAN_DATE", "B_LAST_DEBIT_TRAN_DATE")
			            .withColumnRenamed("IB_LAST_TRAN_DATE", "B_IB_LAST_TRAN_DATE")
			            .withColumnRenamed("ATM_LAST_TRAN_DATE", "B_ATM_LAST_TRAN_DATE")
						.withColumnRenamed("OPENING_TRAN_DATE", "B_OPENING_TRAN_DATE")
						.withColumnRenamed("LR_FREQ_TYPE", "B_LR_FREQ_TYPE")
						.withColumnRenamed("EMIAMT", "B_EMIAMT");

 //Joined C dataset
			Dataset<Row> joined = adf_client
						.join(adf_cdr_am_cd, adf_client.col("ACCOUNT_NO")
						.equalTo(adf_cdr_am_cd.col("B_ACCOUNT_NO")),"left");
			
			
			
//			joined.show();
			
	StructField[] adfClientSchemaFields = {
			new StructField("GL_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("GL_SUB_HEAD_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("ACCOUNT_NO", DataTypes.StringType, true, Metadata.empty()),
			new StructField("ACCOUNT_HOLDER_NAME", DataTypes.StringType, true, Metadata.empty()),
			new StructField("CUSTOMER_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("OPENING_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("PRODUCT_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("CURRENCY_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("CLOSING_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("LINE_ID", DataTypes.StringType, true, Metadata.empty()),
			new StructField("BRANCH_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("DORMANT_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("ACCOUNT_TYPE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("ACCOUNT_PREFIX", DataTypes.StringType, true, Metadata.empty()),
			new StructField("PSL_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("PSL_DESC", DataTypes.StringType, true, Metadata.empty()),
			new StructField("MATURITY_AMOUNT", DataTypes.StringType, true, Metadata.empty()),
			new StructField("AMOUNT", DataTypes.StringType, true, Metadata.empty()),
			new StructField("BOOKING_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("MATURITY_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("INTEREST_RATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("REF_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("REF_DESC", DataTypes.StringType, true, Metadata.empty()),
			new StructField("ADVANCE_TYPE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("ADVANCE_PURPOSE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("PP20_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("PP20_DESC", DataTypes.StringType, true, Metadata.empty()),
			new StructField("ROI_TYPE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("BORROWER_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("OCCUPATION_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("OCCUPATION_DESC", DataTypes.StringType, true, Metadata.empty()),
			new StructField("BSR3_COMMODITY_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("SUBSIDY", DataTypes.StringType, true, Metadata.empty()),
			new StructField("INITIAL_SANCTION_LIMIT", DataTypes.StringType, true, Metadata.empty()),
			new StructField("INITIAL_SANCTION_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("INVESTMENT_FLAG", DataTypes.StringType, true, Metadata.empty()),
			new StructField("LAST_CREDIT_TRAN_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("LAST_DEBIT_TRAN_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("LOAN_END_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("OPENING_TRAN_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("ACCT_BUSINESS_SEGMENT", DataTypes.StringType, true, Metadata.empty()),
			new StructField("INITIAL_FUNDING", DataTypes.StringType, true, Metadata.empty()),
			new StructField("RELIGION_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("RELIGION", DataTypes.StringType, true, Metadata.empty()),
			new StructField("BOOKING_DATE_1", DataTypes.StringType, true, Metadata.empty()),
			new StructField("LIVE_CLOSED", DataTypes.StringType, true, Metadata.empty()),
			new StructField("DEPOSIT_STATUS", DataTypes.StringType, true, Metadata.empty()),
			new StructField("SCHEME_DESCRIPTION", DataTypes.StringType, true, Metadata.empty()),
			new StructField("RM", DataTypes.StringType, true, Metadata.empty()),
			new StructField("MODE_OF_OPERATION", DataTypes.StringType, true, Metadata.empty()),
			new StructField("MODE_OF_OPERATION_DESC", DataTypes.StringType, true, Metadata.empty()),
			new StructField("BAR_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("ASSET_CLASSIFICATION", DataTypes.StringType, true, Metadata.empty()),
			new StructField("PROMO_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("DEBIT_CARD", DataTypes.StringType, true, Metadata.empty()),
			new StructField("NPA_FLG", DataTypes.StringType, true, Metadata.empty()),
			new StructField("NPA_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("BRANCH_NAME", DataTypes.StringType, true, Metadata.empty()),
			new StructField("LC_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("LG_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("BUSINESS_SEG_DESC", DataTypes.StringType, true, Metadata.empty()),
			new StructField("ACCOUNT_OWNERSHIP", DataTypes.StringType, true, Metadata.empty()),
			new StructField("CHEQ_ALLOWED", DataTypes.StringType, true, Metadata.empty()),
			new StructField("FREEZE_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("FREEZE_DESC", DataTypes.StringType, true, Metadata.empty()),
			new StructField("FREEZE_REASON_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("FREEZE_REASON_DESC", DataTypes.StringType, true, Metadata.empty()),
			new StructField("RISK_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("ENTERER_ID", DataTypes.StringType, true, Metadata.empty()),
			new StructField("INACTIVE_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("IB_LAST_TRAN_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("ATM_LAST_TRAN_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("LOAN_PAYOFF_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("REASON_FOR_CLOSURE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("EMIAMT", DataTypes.StringType, true, Metadata.empty()),
			new StructField("TD_RENEW_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("MODE_OF_OPER_CODE", DataTypes.StringType, true, Metadata.empty()), 
			new StructField("AQB_WAIVER", DataTypes.StringType, true, Metadata.empty()),
			new StructField("RATE_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("EMI_START_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("ACID", DataTypes.StringType, true, Metadata.empty()),
			new StructField("LINK_OPER_ACCOUNT", DataTypes.StringType, true, Metadata.empty()),
			new StructField("ACCOUNT_STATUS", DataTypes.StringType, true, Metadata.empty()),
			new StructField("GOV_SCHEME_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("ROI_CARD_RATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("PAYOFF_FLG", DataTypes.StringType, true, Metadata.empty()),
			new StructField("LR_FREQ_TYPE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("APP_REF_NO", DataTypes.StringType, true, Metadata.empty()),
			new StructField("PREFERED_ACCOUNT", DataTypes.StringType, true, Metadata.empty()),
			new StructField("CHRG_OFF_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("INTEREST_RESET_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("CC_RENEWAL_DATE", DataTypes.StringType, true, Metadata.empty()), 
			new StructField("PTC_FLAG", DataTypes.StringType, true, Metadata.empty()),
			new StructField("GROUP_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("URN_NO", DataTypes.StringType, true, Metadata.empty()),
			new StructField("RENEWAL_FLAG", DataTypes.StringType, true, Metadata.empty()),
			new StructField("CHARGE_OFF_FLG", DataTypes.StringType, true, Metadata.empty()),
			new StructField("DEPOSIT_PERIOD_MTHS", DataTypes.StringType, true, Metadata.empty()),
			new StructField("DEPOSIT_PERIOD_DAYS", DataTypes.StringType, true, Metadata.empty()),
			new StructField("DSB_FLAG", DataTypes.StringType, true, Metadata.empty()),
			new StructField("PROMO_CODE_N", DataTypes.StringType, true, Metadata.empty()),
			new StructField("RISK_CATEGORY_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("RISK_DESC", DataTypes.StringType, true, Metadata.empty()),
			new StructField("CUSTOMERNREFLG", DataTypes.StringType, true, Metadata.empty()),
			new StructField("LIMIT_REVIEW_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("LIMIT_EXPIRY_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("FIRST_EMI_START_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("DRAWING_POWER", DataTypes.StringType, true, Metadata.empty()),
			new StructField("DRAWING_POWER_IND", DataTypes.StringType, true, Metadata.empty()),
			new StructField("LCHG_USER_ID", DataTypes.StringType, true, Metadata.empty()),
			new StructField("RCRE_USER_ID", DataTypes.StringType, true, Metadata.empty()),
			new StructField("SCHEME_TYPE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("INDUSTRY_TYPE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("NEXT_EMI_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("LINKED_SB_ACID", DataTypes.StringType, true, Metadata.empty()),
			new StructField("SOURCE_DEAL_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("DISBURSAL_DEAL_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("CLOSURE_FLAG", DataTypes.StringType, true, Metadata.empty()),
			new StructField("LAST_FREZ_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("ACCT_STATUS_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("REASON_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("LAST_TRAN_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("PEG_REVIEW_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("APPLICABLE_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("ACCT_LVL_GROUPING", DataTypes.StringType, true, Metadata.empty()),
			new StructField("AADHAR_SEEDING_YN", DataTypes.StringType, true, Metadata.empty()),
			new StructField("PROJECT_FINANCE_YN", DataTypes.StringType, true, Metadata.empty()),
			new StructField("CHRGE_OFF_DATE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("DRWNG_POWER", DataTypes.StringType, true, Metadata.empty()),
			new StructField("DRWNG_POWER_IND", DataTypes.StringType, true, Metadata.empty()),
			new StructField("SCHM_TYPE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("DISBURSE_DEAL_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("CHRG_LEVEL_CODE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("NOMINATION", DataTypes.StringType, true, Metadata.empty()),
			new StructField("SIGNATURE_STATUS", DataTypes.StringType, true, Metadata.empty()),
			new StructField("EMI_TYPE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("PAYMENT_METHOD", DataTypes.StringType, true, Metadata.empty()),
			new StructField("CARD_TYPE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("CARD_SUB_TYPE", DataTypes.StringType, true, Metadata.empty()),
			new StructField("REPRICING_PLAN", DataTypes.StringType, true, Metadata.empty()),
			new StructField("REPAYMENT_ACID", DataTypes.StringType, true, Metadata.empty())
			};
	StructType adfClientSchema = new StructType(adfClientSchemaFields);
	ExpressionEncoder<Row> client_encoder = RowEncoder.apply(adfClientSchema);
	
	Dataset<Row> client_account_master  = joined.flatMap(new FlatMapFunction<Row, Row>() {
		@Override
		public Iterator<Row> call(Row record) throws Exception {
			String ACID = record.getAs("ACID")==null?null:record.getAs("ACID");
			String GL_CODE = record.getAs("GL_CODE");
			String GL_SUB_HEAD_CODE = record.getAs("GL_SUB_HEAD_CODE");
			String ACCOUNT_NO = record.getAs("ACCOUNT_NO");
//			String ACCT_NAME = record.getAs("ACCT_NAME");
			String ACCOUNT_HOLDER_NAME = record.getAs("ACCOUNT_HOLDER_NAME");
			String CUSTOMER_CODE = record.getAs("CUSTOMER_CODE");
			String OPENING_DATE = record.getAs("OPENING_DATE");
			String PRODUCT_CODE = record.getAs("PRODUCT_CODE");
			String CURRENCY_CODE = record.getAs("CURRENCY_CODE");
			String CLOSING_DATE = record.getAs("CLOSING_DATE");
			String LINE_ID = record.getAs("LINE_ID");
			String BRANCH_CODE = record.getAs("BRANCH_CODE");
			String DORMANT_DATE = record.getAs("DORMANT_DATE");
			String ACCOUNT_TYPE = record.getAs("ACCOUNT_TYPE");
			String ACCOUNT_PREFIX = record.getAs("ACCOUNT_PREFIX");
			String PSL_CODE = record.getAs("PSL_CODE");
			String PSL_DESC = record.getAs("PSL_DESC");
			String RELIGION_CODE = record.getAs("RELIGION_CODE");
			String RELIGION = record.getAs("RELIGION");
			String MATURITY_AMOUNT = record.getAs("MATURITY_AMOUNT");
			String AMOUNT = record.getAs("AMOUNT");
			String MATURITY_DATE = record.getAs("MATURITY_DATE");
			String INTEREST_RATE = record.getAs("INTEREST_RATE");
			String PP20_CODE = record.getAs("PP20_CODE");
			String PP20_DESC = record.getAs("PP20_DESC");
			String ADVANCE_TYPE = record.getAs("ADVANCE_TYPE");
			String ADVANCE_PURPOSE = record.getAs("ADVANCE_PURPOSE");
			String ROI_TYPE = record.getAs("ROI_TYPE");
			String BORROWER_CODE = record.getAs("BORROWER_CODE");
			String OCCUPATION_CODE = record.getAs("OCCUPATION_CODE");
			String OCCUPATION_DESC = record.getAs("OCCUPATION_DESC");
			String BSR3_COMMODITY_CODE = record.getAs("BSR3_COMMODITY_CODE");
			String SUBSIDY = record.getAs("SUBSIDY");
			String INITIAL_SANCTION_LIMIT = record.getAs("INITIAL_SANCTION_LIMIT");
			String INITIAL_SANCTION_DATE = record.getAs("INITIAL_SANCTION_DATE");
			String INVESTMENT_FLAG = record.getAs("INVESTMENT_FLAG");
			String LAST_CREDIT_TRAN_DATE = record.getAs("LAST_CREDIT_TRAN_DATE");
			String LAST_DEBIT_TRAN_DATE = record.getAs("LAST_DEBIT_TRAN_DATE");
			String INITIAL_FUNDING = record.getAs("INITIAL_FUNDING");
			String OPENING_TRAN_DATE = record.getAs("OPENING_TRAN_DATE");
			String ACCT_BUSINESS_SEGMENT = record.getAs("ACCT_BUSINESS_SEGMENT");
			String BOOKING_DATE = record.getAs("BOOKING_DATE");
			String LIVE_CLOSED = record.getAs("LIVE_CLOSED");
			String DEPOSIT_STATUS = record.getAs("DEPOSIT_STATUS");
			String SCHEME_DESCRIPTION = record.getAs("SCHEME_DESCRIPTION");
			String RM = record.getAs("RM");
			String MODE_OF_OPERATION = record.getAs("MODE_OF_OPERATION");
			String MODE_OF_OPERATION_DESC = record.getAs("MODE_OF_OPERATION_DESC");
			String BAR_CODE = record.getAs("BAR_CODE");
			String ASSET_CLASSIFICATION = record.getAs("ASSET_CLASSIFICATION");
			String PROMO_CODE = record.getAs("PROMO_CODE");
			String DEBIT_CARD = record.getAs("DEBIT_CARD");
			String NPA_FLG = record.getAs("NPA_FLG");
			String NPA_DATE = record.getAs("NPA_DATE");
			String BRANCH_NAME = record.getAs("BRANCH_NAME");
			String LC_CODE = record.getAs("LC_CODE");
			String LG_CODE = record.getAs("LG_CODE");
			String BUSINESS_SEG_DESC = record.getAs("BUSINESS_SEG_DESC");
			String ACCOUNT_OWNERSHIP = record.getAs("ACCOUNT_OWNERSHIP");
			String CHEQ_ALLOWED = record.getAs("CHEQ_ALLOWED");
			String FREEZE_CODE = record.getAs("FREEZE_CODE");
			String FREEZE_DESC = record.getAs("FREEZE_DESC");
			String FREEZE_REASON_CODE = record.getAs("FREEZE_REASON_CODE");
			String FREEZE_REASON_DESC = record.getAs("FREEZE_REASON_DESC");
			String RISK_CODE = record.getAs("RISK_CODE");
			String ENTERER_ID = record.getAs("ENTERER_ID");
			String INACTIVE_DATE = record.getAs("INACTIVE_DATE");
			String IB_LAST_TRAN_DATE = record.getAs("IB_LAST_TRAN_DATE");
			String ATM_LAST_TRAN_DATE = record.getAs("ATM_LAST_TRAN_DATE");
			String LOAN_PAYOFF_DATE = record.getAs("LOAN_PAYOFF_DATE");
			String REASON_FOR_CLOSURE = record.getAs("REASON_FOR_CLOSURE");
			String EMIAMT = record.getAs("EMIAMT");
			String AQB_WAIVER = record.getAs("AQB_WAIVER");
			String RATE_CODE = record.getAs("RATE_CODE");
			String EMI_START_DATE = record.getAs("EMI_START_DATE");
			String LINK_OPER_ACCOUNT = record.getAs("LINK_OPER_ACCOUNT");
			String ACCOUNT_STATUS = record.getAs("ACCOUNT_STATUS");
			String GOV_SCHEME_CODE = record.getAs("GOV_SCHEME_CODE");
			String ROI_CARD_RATE = record.getAs("ROI_CARD_RATE");
			String PAYOFF_FLG = record.getAs("PAYOFF_FLG");
			String LR_FREQ_TYPE = record.getAs("LR_FREQ_TYPE");
			String APP_REF_NO = record.getAs("APP_REF_NO");
			String PREFERED_ACCOUNT = record.getAs("PREFERED_ACCOUNT");
			String CHRGE_OFF_DATE = record.getAs("CHRGE_OFF_DATE");
			String REASON_CODE = record.getAs("REASON_CODE");
			String CC_RENEWAL_DATE = record.getAs("CC_RENEWAL_DATE");
			String PTC_FLAG = record.getAs("PTC_FLAG");
			String GROUP_CODE = record.getAs("GROUP_CODE");
			String URN_NO = record.getAs("URN_NO");
			String RENEWAL_FLAG = record.getAs("RENEWAL_FLAG");
			String CHARGE_OFF_FLG = record.getAs("CHARGE_OFF_FLG");
			String DEPOSIT_PERIOD_MTHS = record.getAs("DEPOSIT_PERIOD_MTHS");
			String DEPOSIT_PERIOD_DAYS = record.getAs("DEPOSIT_PERIOD_DAYS");
			String DSB_FLAG = record.getAs("DSB_FLAG");
			String PROMO_CODE_N = record.getAs("PROMO_CODE_N");
			String RISK_CATEGORY_CODE = record.getAs("RISK_CATEGORY_CODE");
			String RISK_DESC = record.getAs("RISK_DESC");
			String CUSTOMERNREFLG = record.getAs("CUSTOMERNREFLG");
			String LIMIT_REVIEW_DATE = record.getAs("LIMIT_REVIEW_DATE");
			String LIMIT_EXPIRY_DATE = record.getAs("LIMIT_EXPIRY_DATE");
			String DRWNG_POWER = record.getAs("DRWNG_POWER");
			String DRWNG_POWER_IND = record.getAs("DRWNG_POWER_IND");
			String LCHG_USER_ID = record.getAs("LCHG_USER_ID");
			String RCRE_USER_ID = record.getAs("RCRE_USER_ID");
			String SCHM_TYPE = record.getAs("SCHM_TYPE");
			String INDUSTRY_TYPE = record.getAs("INDUSTRY_TYPE");
			String LINKED_SB_ACID = record.getAs("LINKED_SB_ACID");
			String SOURCE_DEAL_CODE = record.getAs("SOURCE_DEAL_CODE");
			String DISBURSE_DEAL_CODE = record.getAs("DISBURSE_DEAL_CODE");
			String CLOSURE_FLAG = record.getAs("CLOSURE_FLAG");
			String LAST_FREZ_DATE = record.getAs("LAST_FREZ_DATE");
			String ACCT_STATUS_DATE = record.getAs("ACCT_STATUS_DATE");
			String LAST_TRAN_DATE = record.getAs("LAST_TRAN_DATE");
			String PEG_REVIEW_DATE = record.getAs("PEG_REVIEW_DATE");
			String APPLICABLE_DATE = record.getAs("APPLICABLE_DATE");
			String ACCT_LVL_GROUPING = record.getAs("ACCT_LVL_GROUPING");
			String AADHAR_SEEDING_YN = record.getAs("AADHAR_SEEDING_YN");
			String PROJECT_FINANCE_YN = record.getAs("PROJECT_FINANCE_YN");
			String CHRG_LEVEL_CODE = record.getAs("CHRG_LEVEL_CODE");
			String SIGNATURE_STATUS = record.getAs("SIGNATURE_STATUS");
			String EMI_TYPE = record.getAs("EMI_TYPE");
			String PAYMENT_METHOD = record.getAs("PAYMENT_METHOD");
			String REPRICING_PLAN = record.getAs("REPRICING_PLAN");
			String REPAYMENT_ACID = record.getAs("REPAYMENT_ACID"); 
			
//new fileds
			String REF_CODE = record.getAs("REF_CODE");
			String REF_DESC = record.getAs("REF_DESC");
			String LOAN_END_DATE = record.getAs("LOAN_END_DATE");
			String BOOKING_DATE_1 = record.getAs("BOOKING_DATE_1");
			String TD_RENEW_DATE = record.getAs("TD_RENEW_DATE");
			String MODE_OF_OPER_CODE = record.getAs("MODE_OF_OPER_CODE");
			String CHRG_OFF_DATE = record.getAs("CHRG_OFF_DATE");
			String DRAWING_POWER = record.getAs("DRAWING_POWER");
			String DRAWING_POWER_IND = record.getAs("DRAWING_POWER_IND");
			String SCHEME_TYPE = record.getAs("SCHEME_TYPE");
			String INTEREST_RESET_DATE = record.getAs("INTEREST_RESET_DATE");
			String FIRST_EMI_START_DATE = record.getAs("FIRST_EMI_START_DATE");
			String NEXT_EMI_DATE = record.getAs("NEXT_EMI_DATE");
			String DISBURSAL_DEAL_CODE = record.getAs("DISBURSAL_DEAL_CODE");
			String NOMINATION = record.getAs("NOMINATION");
			String CARD_TYPE = record.getAs("CARD_TYPE");
			String CARD_SUB_TYPE = record.getAs("CARD_SUB_TYPE");
//nf				

			String B_ACCOUNT_NO = record.getAs("B_ACCOUNT_NO");
			String B_INITIAL_FUNDING = record.getAs("B_INITIAL_FUNDING");
			String B_LAST_CREDIT_TRAN_DATE = record.getAs("B_LAST_CREDIT_TRAN_DATE");
			String B_LAST_DEBIT_TRAN_DATE = record.getAs("B_LAST_DEBIT_TRAN_DATE");
			String B_IB_LAST_TRAN_DATE = record.getAs("B_IB_LAST_TRAN_DATE");
			String B_ATM_LAST_TRAN_DATE = record.getAs("B_ATM_LAST_TRAN_DATE");
			String B_OPENING_TRAN_DATE = record.getAs("B_OPENING_TRAN_DATE");
			String B_LR_FREQ_TYPE = record.getAs("B_LR_FREQ_TYPE");
			String B_EMIAMT = record.getAs("B_EMIAMT");

				if (B_ACCOUNT_NO != null) {
				INITIAL_FUNDING = B_INITIAL_FUNDING;
				LAST_CREDIT_TRAN_DATE = B_LAST_CREDIT_TRAN_DATE;
				LAST_DEBIT_TRAN_DATE = B_LAST_DEBIT_TRAN_DATE;
				IB_LAST_TRAN_DATE = B_IB_LAST_TRAN_DATE;
				ATM_LAST_TRAN_DATE = B_ATM_LAST_TRAN_DATE;
				OPENING_TRAN_DATE = B_OPENING_TRAN_DATE;
				LR_FREQ_TYPE = B_LR_FREQ_TYPE;
				EMIAMT = B_EMIAMT;
				}
			
			
			ArrayList<Row> newRows = new ArrayList<>();
			Row recordOut;
			recordOut = RowFactory.create(GL_CODE,GL_SUB_HEAD_CODE,ACCOUNT_NO,ACCOUNT_HOLDER_NAME,CUSTOMER_CODE,OPENING_DATE,PRODUCT_CODE,CURRENCY_CODE,CLOSING_DATE,LINE_ID,BRANCH_CODE,DORMANT_DATE,ACCOUNT_TYPE,ACCOUNT_PREFIX,PSL_CODE,PSL_DESC,MATURITY_AMOUNT,AMOUNT,BOOKING_DATE,
					MATURITY_DATE,INTEREST_RATE,REF_CODE,REF_DESC,ADVANCE_TYPE,ADVANCE_PURPOSE,PP20_CODE,PP20_DESC,ROI_TYPE,BORROWER_CODE,
					OCCUPATION_CODE,OCCUPATION_DESC,BSR3_COMMODITY_CODE,SUBSIDY,INITIAL_SANCTION_LIMIT,INITIAL_SANCTION_DATE,INVESTMENT_FLAG,
					LAST_CREDIT_TRAN_DATE,LAST_DEBIT_TRAN_DATE,LOAN_END_DATE,OPENING_TRAN_DATE,ACCT_BUSINESS_SEGMENT,INITIAL_FUNDING,RELIGION_CODE,RELIGION,BOOKING_DATE_1,LIVE_CLOSED,DEPOSIT_STATUS,SCHEME_DESCRIPTION,RM,MODE_OF_OPERATION,MODE_OF_OPERATION_DESC,BAR_CODE,ASSET_CLASSIFICATION,PROMO_CODE,DEBIT_CARD,NPA_FLG,NPA_DATE,BRANCH_NAME,LC_CODE,LG_CODE,
					BUSINESS_SEG_DESC,ACCOUNT_OWNERSHIP,CHEQ_ALLOWED,FREEZE_CODE,FREEZE_DESC,FREEZE_REASON_CODE,FREEZE_REASON_DESC,RISK_CODE,
					ENTERER_ID,INACTIVE_DATE,IB_LAST_TRAN_DATE,ATM_LAST_TRAN_DATE,LOAN_PAYOFF_DATE,REASON_FOR_CLOSURE,EMIAMT,TD_RENEW_DATE,
					MODE_OF_OPER_CODE, AQB_WAIVER,RATE_CODE,EMI_START_DATE,ACID,LINK_OPER_ACCOUNT,ACCOUNT_STATUS,GOV_SCHEME_CODE,ROI_CARD_RATE,
					PAYOFF_FLG,LR_FREQ_TYPE,APP_REF_NO,PREFERED_ACCOUNT,CHRG_OFF_DATE,INTEREST_RESET_DATE,CC_RENEWAL_DATE, PTC_FLAG,GROUP_CODE,
					URN_NO,RENEWAL_FLAG,CHARGE_OFF_FLG,DEPOSIT_PERIOD_MTHS,DEPOSIT_PERIOD_DAYS,DSB_FLAG,PROMO_CODE_N,RISK_CATEGORY_CODE,
					RISK_DESC,CUSTOMERNREFLG,LIMIT_REVIEW_DATE,LIMIT_EXPIRY_DATE,FIRST_EMI_START_DATE,DRAWING_POWER,DRAWING_POWER_IND,
					LCHG_USER_ID,RCRE_USER_ID,SCHEME_TYPE,INDUSTRY_TYPE,NEXT_EMI_DATE,LINKED_SB_ACID,SOURCE_DEAL_CODE,DISBURSAL_DEAL_CODE,
					CLOSURE_FLAG,LAST_FREZ_DATE,ACCT_STATUS_DATE,REASON_CODE,LAST_TRAN_DATE,PEG_REVIEW_DATE,APPLICABLE_DATE,ACCT_LVL_GROUPING,
					AADHAR_SEEDING_YN,PROJECT_FINANCE_YN,CHRGE_OFF_DATE,DRWNG_POWER,DRWNG_POWER_IND,SCHM_TYPE,DISBURSE_DEAL_CODE,
					CHRG_LEVEL_CODE,NOMINATION,SIGNATURE_STATUS,EMI_TYPE,PAYMENT_METHOD,CARD_TYPE,CARD_SUB_TYPE,REPRICING_PLAN,REPAYMENT_ACID);				newRows.add(recordOut);
			return newRows.iterator();
		}
	},client_encoder);
	client_account_master.show();
	client_account_master.printSchema();//remove this comment later
	System.out.println(" No of rows in client_account_master= " + client_account_master.count() + "  & no of colums  = " + client_account_master.columns().length);

//	client_account_master.show();		
	
	// A QUERY STARTING 
	
	
//	Dataset<Row> C_GAM = spark.read()
//			   .format("com.databricks.spark.csv")
//			   .option("delimiter", "|")
//			   .option("header", "true")
//			   .load("resources/C_GAM.txt");
//		
//	//dataset for ALR
//	Dataset<Row> ALR = spark.read()
//			   .format("com.databricks.spark.csv")
//			   .option("delimiter", "|")
//			   .option("header", "true")
//			   .load("resources/ALR.txt");
//	
//	Dataset<Row> dv2 = C_GAM.join(ALR, C_GAM.col("ACID")
//            .equalTo(ALR.col("ACID")))        
//            .drop(ALR.col("ACID"))
//            .drop(ALR.col("RCRE_TIME"))
//            .filter(ALR.col("ACCT_LABEL").contains("INSTAKIT")).filter(ALR.col("ENTITY_CRE_FLG").equalTo("Y")).filter(ALR.col("DEL_FLG").equalTo("N"));
//	//dv2.show();
//	
//	Dataset<Row> rkit_acct = dv2.select(dv2.col("ACID"),dv2.col("FORACID"),
//										dv2.col("ACCT_OPN_DATE"),dv2.col("RCRE_TIME"),dv2.col("ACCT_NAME"),
//										dv2.col("ACCT_CLS_DATE"))
//								.withColumnRenamed("ACID", "D_ACID")
//								.withColumnRenamed("FORACID", "D_FORACID")
//								.withColumnRenamed("RCRE_TIME", "D_RCRE_TIME")
//								.withColumnRenamed("ACCT_OPN_DATE", "D_ACCT_OPN_DATE")
//								.withColumnRenamed("ACCT_NAME", "D_ACCT_NAME")
//								.withColumnRenamed("ACCT_CLS_DATE", "D_ACCT_CLS_DATE");
//   // System.out.println("rkt cols = " + rkit_acct.columns().length);
//   // Joined D dataset
//	Dataset<Row> client_account_master_rkit = client_account_master
//							.join(rkit_acct, client_account_master.col("ACCOUNT_NO")
//							.equalTo(rkit_acct.col("D_FORACID")),"left");
//	
//	//joined_D.show();
//	Dataset<Row> client_am_rkit_df  = client_account_master_rkit.flatMap(new FlatMapFunction<Row, Row>() {
//		@Override
//		public Iterator<Row> call(Row record) throws Exception {
//			String ACID = record.getAs("ACID");
//			String GL_CODE = record.getAs("GL_CODE");
//			String GL_SUB_HEAD_CODE = record.getAs("GL_SUB_HEAD_CODE");
//			String ACCOUNT_NO = record.getAs("ACCOUNT_NO");
//	//		String ACCT_NAME = record.getAs("ACCT_NAME");
//			String ACCOUNT_HOLDER_NAME = record.getAs("ACCOUNT_HOLDER_NAME");
//			String CUSTOMER_CODE = record.getAs("CUSTOMER_CODE");
//			String OPENING_DATE = record.getAs("OPENING_DATE");
//			String PRODUCT_CODE = record.getAs("PRODUCT_CODE");
//			String CURRENCY_CODE = record.getAs("CURRENCY_CODE");
//			String CLOSING_DATE = record.getAs("CLOSING_DATE");
//			String LINE_ID = record.getAs("LINE_ID");
//			String BRANCH_CODE = record.getAs("BRANCH_CODE");
//			String DORMANT_DATE = record.getAs("DORMANT_DATE");
//			String ACCOUNT_TYPE = record.getAs("ACCOUNT_TYPE");
//			String ACCOUNT_PREFIX = record.getAs("ACCOUNT_PREFIX");
//			String PSL_CODE = record.getAs("PSL_CODE");
//			String PSL_DESC = record.getAs("PSL_DESC");
//			String RELIGION_CODE = record.getAs("RELIGION_CODE");
//			String RELIGION = record.getAs("RELIGION");
//			String MATURITY_AMOUNT = record.getAs("MATURITY_AMOUNT");
//			String AMOUNT = record.getAs("AMOUNT");
//			String MATURITY_DATE = record.getAs("MATURITY_DATE");
//			String INTEREST_RATE = record.getAs("INTEREST_RATE");
//			String PP20_CODE = record.getAs("PP20_CODE");
//			String PP20_DESC = record.getAs("PP20_DESC");
//			String ADVANCE_TYPE = record.getAs("ADVANCE_TYPE");
//			String ADVANCE_PURPOSE = record.getAs("ADVANCE_PURPOSE");
//			String ROI_TYPE = record.getAs("ROI_TYPE");
//			String BORROWER_CODE = record.getAs("BORROWER_CODE");
//			String OCCUPATION_CODE = record.getAs("OCCUPATION_CODE");
//			String OCCUPATION_DESC = record.getAs("OCCUPATION_DESC");
//			String BSR3_COMMODITY_CODE = record.getAs("BSR3_COMMODITY_CODE");
//			String SUBSIDY = record.getAs("SUBSIDY");
//			String INITIAL_SANCTION_LIMIT = record.getAs("INITIAL_SANCTION_LIMIT");
//			String INITIAL_SANCTION_DATE = record.getAs("INITIAL_SANCTION_DATE");
//			String INVESTMENT_FLAG = record.getAs("INVESTMENT_FLAG");
//			String LAST_CREDIT_TRAN_DATE = record.getAs("LAST_CREDIT_TRAN_DATE");
//			String LAST_DEBIT_TRAN_DATE = record.getAs("LAST_DEBIT_TRAN_DATE");
//			String INITIAL_FUNDING = record.getAs("INITIAL_FUNDING");
//			String OPENING_TRAN_DATE = record.getAs("OPENING_TRAN_DATE");
//			String ACCT_BUSINESS_SEGMENT = record.getAs("ACCT_BUSINESS_SEGMENT");
//			String BOOKING_DATE = record.getAs("BOOKING_DATE");
//			String LIVE_CLOSED = record.getAs("LIVE_CLOSED");
//			String DEPOSIT_STATUS = record.getAs("DEPOSIT_STATUS");
//			String SCHEME_DESCRIPTION = record.getAs("SCHEME_DESCRIPTION");
//			String RM = record.getAs("RM");
//			String MODE_OF_OPERATION = record.getAs("MODE_OF_OPERATION");
//			String MODE_OF_OPERATION_DESC = record.getAs("MODE_OF_OPERATION_DESC");
//			String BAR_CODE = record.getAs("BAR_CODE");
//			String ASSET_CLASSIFICATION = record.getAs("ASSET_CLASSIFICATION");
//			String PROMO_CODE = record.getAs("PROMO_CODE");
//			String DEBIT_CARD = record.getAs("DEBIT_CARD");
//			String NPA_FLG = record.getAs("NPA_FLG");
//			String NPA_DATE = record.getAs("NPA_DATE");
//			String BRANCH_NAME = record.getAs("BRANCH_NAME");
//			String LC_CODE = record.getAs("LC_CODE");
//			String LG_CODE = record.getAs("LG_CODE");
//			String BUSINESS_SEG_DESC = record.getAs("BUSINESS_SEG_DESC");
//			String ACCOUNT_OWNERSHIP = record.getAs("ACCOUNT_OWNERSHIP");
//			String CHEQ_ALLOWED = record.getAs("CHEQ_ALLOWED");
//			String FREEZE_CODE = record.getAs("FREEZE_CODE");
//			String FREEZE_DESC = record.getAs("FREEZE_DESC");
//			String FREEZE_REASON_CODE = record.getAs("FREEZE_REASON_CODE");
//			String FREEZE_REASON_DESC = record.getAs("FREEZE_REASON_DESC");
//			String RISK_CODE = record.getAs("RISK_CODE");
//			String ENTERER_ID = record.getAs("ENTERER_ID");
//			String INACTIVE_DATE = record.getAs("INACTIVE_DATE");
//			String IB_LAST_TRAN_DATE = record.getAs("IB_LAST_TRAN_DATE");
//			String ATM_LAST_TRAN_DATE = record.getAs("ATM_LAST_TRAN_DATE");
//			String LOAN_PAYOFF_DATE = record.getAs("LOAN_PAYOFF_DATE");
//			String REASON_FOR_CLOSURE = record.getAs("REASON_FOR_CLOSURE");
//			String EMIAMT = record.getAs("EMIAMT");
//			String AQB_WAIVER = record.getAs("AQB_WAIVER");
//			String RATE_CODE = record.getAs("RATE_CODE");
//			String EMI_START_DATE = record.getAs("EMI_START_DATE");
//			String LINK_OPER_ACCOUNT = record.getAs("LINK_OPER_ACCOUNT");
//			String ACCOUNT_STATUS = record.getAs("ACCOUNT_STATUS");
//			String GOV_SCHEME_CODE = record.getAs("GOV_SCHEME_CODE");
//			String ROI_CARD_RATE = record.getAs("ROI_CARD_RATE");
//			String PAYOFF_FLG = record.getAs("PAYOFF_FLG");
//			String LR_FREQ_TYPE = record.getAs("LR_FREQ_TYPE");
//			String APP_REF_NO = record.getAs("APP_REF_NO");
//			String PREFERED_ACCOUNT = record.getAs("PREFERED_ACCOUNT");
//			String CHRGE_OFF_DATE = record.getAs("CHRGE_OFF_DATE");
//			String REASON_CODE = record.getAs("REASON_CODE");
//			String CC_RENEWAL_DATE = record.getAs("CC_RENEWAL_DATE");
//			String PTC_FLAG = record.getAs("PTC_FLAG");
//			String GROUP_CODE = record.getAs("GROUP_CODE");
//			String URN_NO = record.getAs("URN_NO");
//			String RENEWAL_FLAG = record.getAs("RENEWAL_FLAG");
//			String CHARGE_OFF_FLG = record.getAs("CHARGE_OFF_FLG");
//			String DEPOSIT_PERIOD_MTHS = record.getAs("DEPOSIT_PERIOD_MTHS");
//			String DEPOSIT_PERIOD_DAYS = record.getAs("DEPOSIT_PERIOD_DAYS");
//			String DSB_FLAG = record.getAs("DSB_FLAG");
//			String PROMO_CODE_N = record.getAs("PROMO_CODE_N");
//			String RISK_CATEGORY_CODE = record.getAs("RISK_CATEGORY_CODE");
//			String RISK_DESC = record.getAs("RISK_DESC");
//			String CUSTOMERNREFLG = record.getAs("CUSTOMERNREFLG");
//			String LIMIT_REVIEW_DATE = record.getAs("LIMIT_REVIEW_DATE");
//			String LIMIT_EXPIRY_DATE = record.getAs("LIMIT_EXPIRY_DATE");
//			String DRWNG_POWER = record.getAs("DRWNG_POWER");
//			String DRWNG_POWER_IND = record.getAs("DRWNG_POWER_IND");
//			String LCHG_USER_ID = record.getAs("LCHG_USER_ID");
//			String RCRE_USER_ID = record.getAs("RCRE_USER_ID");
//			String SCHM_TYPE = record.getAs("SCHM_TYPE");
//			String INDUSTRY_TYPE = record.getAs("INDUSTRY_TYPE");
//			String LINKED_SB_ACID = record.getAs("LINKED_SB_ACID");
//			String SOURCE_DEAL_CODE = record.getAs("SOURCE_DEAL_CODE");
//			String DISBURSE_DEAL_CODE = record.getAs("DISBURSE_DEAL_CODE");
//			String CLOSURE_FLAG = record.getAs("CLOSURE_FLAG");
//			String LAST_FREZ_DATE = record.getAs("LAST_FREZ_DATE");
//			String ACCT_STATUS_DATE = record.getAs("ACCT_STATUS_DATE");
//			String LAST_TRAN_DATE = record.getAs("LAST_TRAN_DATE");
//			String PEG_REVIEW_DATE = record.getAs("PEG_REVIEW_DATE");
//			String APPLICABLE_DATE = record.getAs("APPLICABLE_DATE");
//			String ACCT_LVL_GROUPING = record.getAs("ACCT_LVL_GROUPING");
//			String AADHAR_SEEDING_YN = record.getAs("AADHAR_SEEDING_YN");
//			String PROJECT_FINANCE_YN = record.getAs("PROJECT_FINANCE_YN");
//			String CHRG_LEVEL_CODE = record.getAs("CHRG_LEVEL_CODE");
//			String SIGNATURE_STATUS = record.getAs("SIGNATURE_STATUS");
//			String EMI_TYPE = record.getAs("EMI_TYPE");
//			String PAYMENT_METHOD = record.getAs("PAYMENT_METHOD");
//			String REPRICING_PLAN = record.getAs("REPRICING_PLAN");
//			String REPAYMENT_ACID = record.getAs("REPAYMENT_ACID");   
////nf
//			String REF_CODE = record.getAs("REF_CODE");
//			String REF_DESC = record.getAs("REF_DESC");
//			String LOAN_END_DATE = record.getAs("LOAN_END_DATE");
//			String BOOKING_DATE_1 = record.getAs("BOOKING_DATE_1");
//			String TD_RENEW_DATE = record.getAs("TD_RENEW_DATE");
//			String MODE_OF_OPER_CODE = record.getAs("MODE_OF_OPER_CODE");
//			String CHRG_OFF_DATE = record.getAs("CHRG_OFF_DATE");
//			String DRAWING_POWER = record.getAs("DRAWING_POWER");
//			String DRAWING_POWER_IND = record.getAs("DRAWING_POWER_IND");
//			String SCHEME_TYPE = record.getAs("SCHEME_TYPE");
//			String INTEREST_RESET_DATE = record.getAs("INTEREST_RESET_DATE");
//			String FIRST_EMI_START_DATE = record.getAs("FIRST_EMI_START_DATE");
//			String NEXT_EMI_DATE = record.getAs("NEXT_EMI_DATE");
//			String DISBURSAL_DEAL_CODE = record.getAs("DISBURSAL_DEAL_CODE");
//			String NOMINATION = record.getAs("NOMINATION");
//			String CARD_TYPE = record.getAs("CARD_TYPE");
//			String CARD_SUB_TYPE = record.getAs("CARD_SUB_TYPE");
//
////nf				
//			
//	//		String D_ACID = record.getAs("D_ACID");
//			String D_FORACID = record.getAs("D_FORACID");
//			String D_RCRE_TIME = record.getAs("D_RCRE_TIME");
//			String D_ACCT_OPN_DATE = record.getAs("D_ACCT_OPN_DATE");
//			String D_ACCT_NAME = record.getAs("D_ACCT_NAME");
//			String D_ACCT_CLS_DATE = record.getAs("D_ACCT_CLS_DATE");	
//		
//		if (D_FORACID != null) {
//				OPENING_TRAN_DATE = D_RCRE_TIME;
//				OPENING_DATE = D_ACCT_OPN_DATE;
//				ACCOUNT_HOLDER_NAME = D_ACCT_NAME;
//				CLOSING_DATE = D_ACCT_CLS_DATE;
//			}
//			
//			ArrayList<Row> newRows = new ArrayList<>();
//			Row recordOut;
//			recordOut = RowFactory.create(GL_CODE,GL_SUB_HEAD_CODE,ACCOUNT_NO,ACCOUNT_HOLDER_NAME,CUSTOMER_CODE,OPENING_DATE,PRODUCT_CODE,CURRENCY_CODE,CLOSING_DATE,LINE_ID,BRANCH_CODE,DORMANT_DATE,ACCOUNT_TYPE,ACCOUNT_PREFIX,PSL_CODE,PSL_DESC,MATURITY_AMOUNT,AMOUNT,BOOKING_DATE,
//					MATURITY_DATE,INTEREST_RATE,REF_CODE,REF_DESC,ADVANCE_TYPE,ADVANCE_PURPOSE,PP20_CODE,PP20_DESC,ROI_TYPE,BORROWER_CODE,
//					OCCUPATION_CODE,OCCUPATION_DESC,BSR3_COMMODITY_CODE,SUBSIDY,INITIAL_SANCTION_LIMIT,INITIAL_SANCTION_DATE,INVESTMENT_FLAG,
//					LAST_CREDIT_TRAN_DATE,LAST_DEBIT_TRAN_DATE,LOAN_END_DATE,OPENING_TRAN_DATE,ACCT_BUSINESS_SEGMENT,INITIAL_FUNDING,RELIGION_CODE,RELIGION,BOOKING_DATE_1,LIVE_CLOSED,DEPOSIT_STATUS,SCHEME_DESCRIPTION,RM,MODE_OF_OPERATION,MODE_OF_OPERATION_DESC,BAR_CODE,ASSET_CLASSIFICATION,PROMO_CODE,DEBIT_CARD,NPA_FLG,NPA_DATE,BRANCH_NAME,LC_CODE,LG_CODE,
//					BUSINESS_SEG_DESC,ACCOUNT_OWNERSHIP,CHEQ_ALLOWED,FREEZE_CODE,FREEZE_DESC,FREEZE_REASON_CODE,FREEZE_REASON_DESC,RISK_CODE,
//					ENTERER_ID,INACTIVE_DATE,IB_LAST_TRAN_DATE,ATM_LAST_TRAN_DATE,LOAN_PAYOFF_DATE,REASON_FOR_CLOSURE,EMIAMT,TD_RENEW_DATE,
//					MODE_OF_OPER_CODE, AQB_WAIVER,RATE_CODE,EMI_START_DATE,ACID,LINK_OPER_ACCOUNT,ACCOUNT_STATUS,GOV_SCHEME_CODE,ROI_CARD_RATE,
//					PAYOFF_FLG,LR_FREQ_TYPE,APP_REF_NO,PREFERED_ACCOUNT,CHRG_OFF_DATE,INTEREST_RESET_DATE,CC_RENEWAL_DATE, PTC_FLAG,GROUP_CODE,
//					URN_NO,RENEWAL_FLAG,CHARGE_OFF_FLG,DEPOSIT_PERIOD_MTHS,DEPOSIT_PERIOD_DAYS,DSB_FLAG,PROMO_CODE_N,RISK_CATEGORY_CODE,
//					RISK_DESC,CUSTOMERNREFLG,LIMIT_REVIEW_DATE,LIMIT_EXPIRY_DATE,FIRST_EMI_START_DATE,DRAWING_POWER,DRAWING_POWER_IND,
//					LCHG_USER_ID,RCRE_USER_ID,SCHEME_TYPE,INDUSTRY_TYPE,NEXT_EMI_DATE,LINKED_SB_ACID,SOURCE_DEAL_CODE,DISBURSAL_DEAL_CODE,
//					CLOSURE_FLAG,LAST_FREZ_DATE,ACCT_STATUS_DATE,REASON_CODE,LAST_TRAN_DATE,PEG_REVIEW_DATE,APPLICABLE_DATE,ACCT_LVL_GROUPING,
//					AADHAR_SEEDING_YN,PROJECT_FINANCE_YN,CHRGE_OFF_DATE,DRWNG_POWER,DRWNG_POWER_IND,SCHM_TYPE,DISBURSE_DEAL_CODE,
//					CHRG_LEVEL_CODE,NOMINATION,SIGNATURE_STATUS,EMI_TYPE,PAYMENT_METHOD,CARD_TYPE,CARD_SUB_TYPE,REPRICING_PLAN,REPAYMENT_ACID);
//			
//			newRows.add(recordOut);
//			return newRows.iterator();
//		}},client_encoder);
//	
//	client_am_rkit_df.show();
//	System.out.println(" No of rows in client_am_rkit_df= " + client_am_rkit_df.count() + "  & no of colums  = " + client_am_rkit_df.columns().length);				
//				
						
						
						
						
						
						
						
						
						
	}

}
