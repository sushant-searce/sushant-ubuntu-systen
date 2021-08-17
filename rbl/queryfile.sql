
Account Master

		1.FINMIS
			
			A.
			
						SELECT C_GAM.ACID,C_GAM.FORACID,C_GAM.ACCT_OPN_DATE,C_GAM.RCRE_TIME,acct_name FROM C_GAM,TBAADM.ALR
			WHERE C_GAM.ACID = ALR.ACID
			AND alr.acct_label like '%INSTAKIT%'
			AND ALR.ENTITY_CRE_FLG='Y' AND ALR.DEL_FLG='N'

			B.
			
						SELECT GAM.ACID,
							   GSH.GL_CODE,
							   GAM.GL_SUB_HEAD_CODE,
							   GAM.FORACID ACCOUNT_NO,
							   GAM.ACCT_NAME ACCOUNT_HOLDER_NAME,
							   GAM.CIF_ID CUSTOMER_CODE,
							   NVL(GAM.ACCT_OPN_DATE, TAM.OPEN_EFFECTIVE_DATE) OPENING_DATE,
							   GAM.SCHM_CODE PRODUCT_CODE,
							   GAM.ACCT_CRNCY_CODE CURRENCY_CODE,
							   GAM.ACCT_CLS_DATE CLOSING_DATE,
							   GAM.LIMIT_B2KID LINE_ID,
							   GAM.SOL_ID BRANCH_CODE,
							   CASE
								 WHEN CAM.ACCT_STATUS = 'D' THEN
								  CAM.ACCT_STATUS_DATE
								 ELSE
								  NULL
							   END DORMANT_DATE,
							   CASE
								 WHEN GSP.ACCT_PREFIX = '70' AND NVL(GSP.PRODUCT_CONCEPT, '') <> '' THEN
								  GSP.PRODUCT_CONCEPT
								 WHEN GSP.PRODUCT_CONCEPT IN ('CC', 'OD') THEN
								  GSP.PRODUCT_CONCEPT
								 ELSE
								  DECODE(GSP.ACCT_PREFIX,
										 '30',
										 'SB',
										 '40',
										 'CA',
										 '60',
										 'CC',
										 '70',
										 'TD',
										 '75',
										 'TD',
										 '80',
										 'RL',
										 '90',
										 'CL')
							   END ACCOUNT_TYPE,
							   GSP.ACCT_PREFIX ACCOUNT_PREFIX,
							   RCT_38.REF_CODE PSL_CODE,
							   RCT_38.REF_DESC PSL_DESC,
							   NULL RELIGION_CODE,
							   NULL RELIGION,
							   TAM.MATURITY_AMOUNT,
							   TAM.DEPOSIT_AMOUNT AMOUNT,
							   NVL(NVL(TAM.MATURITY_DATE, LAM.EI_PERD_END_DATE), ITC.END_DATE) MATURITY_DATE,
							   0 INTEREST_RATE,
							   RCT_AF.REF_CODE PP20_CODE,
							   RCT_AF.REF_DESC PP20_DESC,
							   GAC.TYPE_OF_ADVN ADVANCE_TYPE,
							   GAC.PURPOSE_OF_ADVN ADVANCE_PURPOSE,
							   --CASE WHEN NVL(GAM.repricing_plan,' ')='F' THEN 'FIXED'
								--    WHEN NVL(GAM.repricing_plan,' ') IN ('M','V') THEN 'FLOAT'
								 --   WHEN NVL(GAM.repricing_plan,' ') IN ('N',' ') AND ITC.PEGGED_FLG = 'Y' AND 
								  --      (( ITC.PEG_FREQUENCY_IN_MONTHS=0 AND ITC.PEG_FREQUENCY_IN_DAYS=0) OR
								--           ITC.PEG_FREQUENCY_IN_MONTHS>=999                               OR
								 --        ( ITC.PEG_FREQUENCY_IN_MONTHS=0 AND ITC.PEG_FREQUENCY_IN_DAYS=999))  THEN 'FIXED'
								 --   WHEN NVL(GAM.repricing_plan,' ') IN ('N',' ') AND ITC.PEGGED_FLG = 'Y' THEN 'FLOAT'     
								 --   WHEN NVL(GAM.repricing_plan,' ') IN ('N',' ') AND ITC.PEGGED_FLG = 'N' THEN 'FLOAT'
								 --   ELSE 'NA'
							  -- END  ROI_TYPE,
						CASE WHEN nvl(ITC.PEGGED_FLG,'N') = 'N' AND NVL(GAM.repricing_plan,' ')='C' THEN 'FIXED'
									WHEN nvl(ITC.PEGGED_FLG,'N') = 'Y' AND PR.NEXT_PEG_REVIEW_DATE IS NOT NULL AND PR.NEXT_PEG_REVIEW_DATE<NVL(NVL(TAM.MATURITY_DATE, LAM.EI_PERD_END_DATE), ITC.END_DATE) THEN 'FLOAT'
									WHEN nvl(ITC.PEGGED_FLG,'N') = 'Y' AND PR.NEXT_PEG_REVIEW_DATE IS NOT NULL AND PR.NEXT_PEG_REVIEW_DATE>=NVL(NVL(TAM.MATURITY_DATE, LAM.EI_PERD_END_DATE), ITC.END_DATE) THEN 'FIXED'
									WHEN nvl(ITC.PEGGED_FLG,'N') = 'N' AND NVL(GAM.repricing_plan,' ')!='C' THEN 'FLOAT'
									ELSE 'FLOAT'
							   END  ROI_TYPE,
							   --DECODE(ITC.PEGGED_FLG, 'Y', 'FIXED', 'FLOAT') ROI_TYPE,
							   GAC.BORROWER_CATEGORY_CODE BORROWER_CODE,
							   RCT_OC.REF_CODE OCCUPATION_CODE,
							   RCT_OC.REF_DESC OCCUPATION_DESC,
							   CFCM.FREE_CODE_1 BSR3_COMMODITY_CODE,
							   LA_SAM.PRIN_SBSDY_AMT SUBSIDY,
							   GAM.SANCT_LIM INITIAL_SANCTION_LIMIT,
							   LHT.LIM_SANCT_DATE INITIAL_SANCTION_DATE,
							   GAC.FREE_TEXT_9 INVESTMENT_FLAG,
							   NULL LAST_CREDIT_TRAN_DATE,
							   NULL LAST_DEBIT_TRAN_DATE,
							   NULL INITIAL_FUNDING,
							   GAM.RCRE_TIME OPENING_TRAN_DATE,
							   GAC.FREE_TEXT_10 ACCT_BUSINESS_SEGMENT,
							   NVL(TAM.OPEN_EFFECTIVE_DATE, GAM.ACCT_OPN_DATE) BOOKING_DATE,
							   LIVE_CLOSED,
							   CASE
								 WHEN GAM.SCHM_TYPE = 'TDA' THEN
								  CASE
								 WHEN TAM.MATURITY_DATE <= (SELECT DC_CLS_DATE FROM TBAADM.GCT) AND
									  GAM.ACCT_CLS_DATE IS NULL THEN
								  'OVERDUE'
								 WHEN TAM.DEPOSIT_STATUS = 'O' THEN
								  'OVERDUE'
								 WHEN TAM.DEPOSIT_STATUS = 'P' THEN
								  'PREMATURE WITHDRAWAL'
								 WHEN TAM.DEPOSIT_STATUS = 'A' THEN
								  'AUTOMATIC RENEWAL'
								 WHEN TAM.DEPOSIT_STATUS = 'B' THEN
								  'RENEWAL OF BALANCE'
								 WHEN TAM.DEPOSIT_STATUS = 'R' THEN
								  'RENEWAL'
								 WHEN TAM.DEPOSIT_STATUS = 'E' THEN
								  'EXTENDED'
								 WHEN TAM.DEPOSIT_STATUS = 'T' THEN
								  'TRANSFERRED OUT'
								 WHEN NVL(TAM.DEPOSIT_STATUS, ' ') = ' ' THEN
								  'REGULAR'
								 WHEN TAM.DEPOSIT_STATUS = 'U' THEN
								  'UNCLAIMED'
								 WHEN TAM.DEPOSIT_STATUS = 'N' THEN
								  'NORMAL CLOSURE'
								 ELSE
								  'NA'
							   END ELSE NULL END DEPOSIT_STATUS,
							   GSP.SCHM_DESC AS SCHEME_DESCRIPTION,
							   GAM.ACCT_MGR_USER_ID RM,
							   GAM.MODE_OF_OPER_CODE MODE_OF_OPERATION,
							   RCT_27.REF_DESC AS MODE_OF_OPERATION_DESC,
							   GAC.FREE_TEXT_3 AS BAR_CODE,
							   ACD.MAIN_CLASSIFICATION_USER AS ASSET_CLASSIFICATION,
							   GAC.FREE_CODE_10 AS PROMO_CODE,
							   C_DBC.DEBIT_CARD AS DEBIT_CARD,
							   GAC.PD_FLG AS NPA_FLG,
							   CASE
								 WHEN NVL(GAC.PD_FLG, 'N') = 'Y' THEN
								  GAC.PD_XFER_DATE
								 ELSE
								  NULL
							   END AS NPA_DATE,
							   SOL.BRANCH_NAME,
							   CFCM.FREE_CODE_5 LC_CODE,
							   CFCM.FREE_CODE_6 LG_CODE,
							   RCT_CN.REF_DESC AS BUSINESS_SEG_DESC,
							   GAM.ACCT_OWNERSHIP ACCOUNT_OWNERSHIP,
							   NVL(GAM.CHQ_ALWD_FLG, 'N') CHEQ_ALLOWED,
							   GAM.FREZ_CODE FREEZE_CODE,
							   GAM.FREEZE_DESC,
							   GAM.FREEZE_REASON_CODE,
							   CASE
								 WHEN GAM.FREZ_CODE IS NULL THEN
								  NULL
								 ELSE
								  RCT_31.REF_DESC
							   END FREEZE_REASON_DESC,
							   GAC.FREE_CODE_8 RISK_CODE,
							   GAM.RCRE_USER_ID ENTERER_ID,
							   CASE
								 WHEN CAM.ACCT_STATUS = 'I' THEN
								  CAM.ACCT_STATUS_DATE
								 ELSE
								  NULL
							   END INACTIVE_DATE,
							   NULL IB_LAST_TRAN_DATE,
							   NULL ATM_LAST_TRAN_DATE,
							   CASE
								 WHEN GAM.SCHM_TYPE IN ('LAA', 'CLA') THEN
								  LAM.PAYOFF_DATE
								 ELSE
								  NULL
							   END LOAN_PAYOFF_DATE,
							   NVL(RCT_EY.REF_DESC, RCT_CAM.ACCT_CLS_REASON_DESC) REASON_FOR_CLOSURE,
							   NULL EMIAMT,
							   GAC.FREE_CODE_10 AQB_WAIVER,
							   ITC.INT_TBL_CODE RATE_CODE,
							   LAM.EI_PERD_START_DATE EMI_START_DATE,
							   TAM.LINK_OPER_ACCOUNT AS LINK_OPER_ACCOUNT,
							   COALESCE(CAM.ACCT_STATUS, TAM.ACCT_STATUS, LAM.ACCT_STATUS_FLG) ACCOUNT_STATUS,
							   GAC.FREE_CODE_5 GOV_SCHEME_CODE,
							   ITC.NRML_PCNT_CR ROI_CARD_RATE,
							   LAM.PAYOFF_FLG,
							   NULL LR_FREQ_TYPE,
							   LAM.CRFILE_REF_ID APP_REF_NO,
							   CFCM.FREE_CODE_21 PREFERED_ACCOUNT,
							   V.CHRGE_OFF_DATE,
							   ITC.REASON_CODE,
							   --CASE
								-- WHEN GAM.LIMIT_B2KID IS NOT NULL THEN
							   --   LLT.LIMIT_REVIEW_DATE
								-- ELSE
								--  LHT.LIM_REVIEW_DATE
							   --END CC_RENEWAL_DATE,
								LHT.LIM_REVIEW_DATE AS CC_RENEWAL_DATE,
							   CASE
								 WHEN GSP.ACCT_PREFIX IN ('80', '90') THEN
								  FREE_TEXT_14
								 ELSE
								  'Non-PTC'
							   END AS PTC_FLAG,
							   CASE
								 WHEN GSP.ACCT_PREFIX IN ('30') THEN
								  FREE_TEXT_14
								 ELSE
								  NULL
							   END AS GROUP_CODE,
							   CASE
								 WHEN GSP.ACCT_PREFIX IN ('30') THEN
								  FREE_TEXT_13
								 ELSE
								  NULL
							   END AS URN_NO,
							   TAM.AUTO_RENEWAL_FLG AS RENEWAL_FLAG,
							   NVL(GAC.CHRGE_OFF_FLG, '') AS CHARGE_OFF_FLG,
							   NVL(TAM.DEPOSIT_PERIOD_MTHS, LAM.REP_PERD_MTHS) DEPOSIT_PERIOD_MTHS,
							   NVL(TAM.DEPOSIT_PERIOD_DAYS, LAM.REP_PERD_DAYS) DEPOSIT_PERIOD_DAYS,
							   CFCM.FREE_CODE_20 DSB_FLAG,
							   CFCM.FREE_CODE_22 PROMO_CODE_N,
							   GAC.FREE_CODE_8 RISK_CATEGORY_CODE,
							   RCT_AI.REF_DESC RISK_DESC,
							   NULL CUSTOMERNREFLG,
							   --CASE
								-- WHEN GAM.LIMIT_B2KID IS NOT NULL THEN
								--  LLT.LIMIT_REVIEW_DATE
								-- ELSE
								--  LHT.LIM_REVIEW_DATE
							   --END LIMIT_REVIEW_DATE,
								LHT.LIM_REVIEW_DATE AS LIMIT_REVIEW_DATE,
							   --CASE
							   --  WHEN GAM.LIMIT_B2KID IS NOT NULL THEN
							   --   LLT.LIM_EXP_DATE
							   --  ELSE
							   --   LHT.LIM_EXP_DATE
							  -- END LIMIT_EXPIRY_DATE,
								LHT.LIM_EXP_DATE AS LIMIT_EXPIRY_DATE,
							   GAM.DRWNG_POWER,
							   GAM.DRWNG_POWER_IND,
							   GAM.LCHG_USER_ID,
							   GAM.RCRE_USER_ID,
							   GAM.SCHM_TYPE,
							   GAC.INDUSTRY_TYPE,
							   LAM.OP_ACID LINKED_SB_ACID,
							   GAM.SOURCE_DEAL_CODE,
							   GAM.DISBURSE_DEAL_CODE,
							   NVL(REPLACE(TAM.CLOSE_ON_MATURITY_FLG, ' ', ''), 'N') CLOSURE_FLAG,
							   LAST_FREZ_DATE,
							   CAM.ACCT_STATUS_DATE,
							   GAM.LAST_TRAN_DATE,
							   PR.NEXT_PEG_REVIEW_DATE PEG_REVIEW_DATE,
							   LHT.APPLICABLE_DATE,
							   CASE
								 WHEN SUBSTR(GAC.FREE_CODE_10, 1, 2) = 'FM' THEN
								  GAC.FREE_CODE_10
								 ELSE
								  NULL
							   END ACCT_LVL_GROUPING,
							   CFCM.FREE_CODE_43 AADHAR_SEEDING_YN,
							   GAC.FREE_TEXT_16 AS PROJECT_FINANCE_YN,
							   CHRG_LEVEL_CODE,
							   S.SIGNATURE_STATUS,
							   LAM.EMI_TYPE,
							   LAM.PAYMENT_METHOD,
							   repricing_plan, /* NEW FIELDS */
							   REPAYMENT_ACID  /* NEW FIELDS */
						  FROM (SELECT GAM.ACID,
									   GAM.GL_SUB_HEAD_CODE,
									   GAM.FORACID,
									   GAM.ACCT_NAME,
									   GAM.CIF_ID,
									   GAM.ACCT_OPN_DATE,
									   GAM.SCHM_CODE,
									   GAM.ACCT_CRNCY_CODE,
									   GAM.ACCT_CLS_DATE,
									   GAM.LIMIT_B2KID,
									   GAM.SOL_ID,
									   GAM.SANCT_LIM,
									   CASE
										 WHEN (SELECT DC_CLS_DATE FROM TBAADM.GCT) >
											  TO_DATE('03-05-2016', 'DD-MM-YYYY') THEN
										  TO_DATE(TO_CHAR(GEM.ACCT_OPN_BOD_DATE, 'DD-MM-YYYY'),
												  'DD-MM-YYYY')
										 ELSE
										  GAM.RCRE_TIME
									   END RCRE_TIME,
									   CASE
										 WHEN GAM.ACCT_CLS_DATE IS NULL OR GAM.ACCT_CLS_DATE = '' THEN
										  'Live'
										 ELSE
										  'Closed'
									   END AS LIVE_CLOSED,
									   GAM.SCHM_TYPE,
									   GAM.ACCT_MGR_USER_ID,
									   GAM.MODE_OF_OPER_CODE,
									   GAM.ACCT_OWNERSHIP,
									   NVL(GAM.CHQ_ALWD_FLG, 'N') CHQ_ALWD_FLG,
									   GAM.FREZ_CODE,
									   GAM.CHRG_LEVEL_CODE,
									   NVL(DECODE(GAM.FREZ_CODE,
												  ' ',
												  'NO FREEZE',
												  'D',
												  'DEBIT FREEZE',
												  'C',
												  'CREDIT FREEZE',
												  'T',
												  'TOTAL FREEZE'),
										   'NA') FREEZE_DESC,
									   FREZ_REASON_CODE,
									   CASE
										 WHEN GAM.FREZ_CODE IS NULL THEN
										  NULL
										 ELSE
										  GAM.FREZ_REASON_CODE
									   END FREEZE_REASON_CODE,
									   GAM.RCRE_USER_ID,
									   GAM.DRWNG_POWER,
									   GAM.DRWNG_POWER_IND,
									   GAM.LCHG_USER_ID,
									   GAM.SOURCE_DEAL_CODE,
									   GAM.DISBURSE_DEAL_CODE,
									   GAM.LAST_TRAN_DATE,
									   CASE
										 WHEN GAM.FREZ_CODE IS NULL THEN
										  NULL
										 ELSE
										  GAM.LAST_FREZ_DATE
									   END LAST_FREZ_DATE,
									   p.repricing_plan 
								  FROM C_GAM GAM
							   INNER JOIN (SELECT ACID
													  FROM C_GAM GAM
													 WHERE trunc(GAM.LCHG_TIME) >=trunc(SYSDATE-2)
													   AND GAM.ACCT_OWNERSHIP <> 'O'
													   AND GAM.DEL_FLG = 'N'
													   AND GAM.ENTITY_CRE_FLG = 'Y'
													UNION 
													SELECT ACID

                                                    

													  FROM TBAADM.ADT
													 WHERE AUDIT_bod_DATE >= trunc(SYSDATE-2)
													   AND ACID!='!') Q ON GAM.ACID = Q.ACID 
								  LEFT JOIN TBAADM.GEM ON GAM.ACID = GEM.ACID
								  LEFT JOIN tbaadm.lrp p ON gam.acid=p.acid AND p.entity_cre_flg='Y' AND p.del_flg='N' /* MODIFICATION */
								 WHERE GAM.ACCT_OWNERSHIP <> 'O'
								   AND GAM.DEL_FLG = 'N'
								   AND GAM.ENTITY_CRE_FLG = 'Y') GAM
						INNER JOIN (SELECT SCHM_CODE, ACCT_PREFIX, SCHM_DESC, PRODUCT_CONCEPT
									   FROM TBAADM.GSP
									  WHERE DEL_FLG = 'N') GSP ON GAM.SCHM_CODE = GSP.SCHM_CODE
						INNER JOIN (SELECT DISTINCT GL_CODE, GL_SUB_HEAD_CODE
									   FROM TBAADM.GSH
									  WHERE GSH.DEL_FLG = 'N') GSH ON GAM.GL_SUB_HEAD_CODE =
																	  GSH.GL_SUB_HEAD_CODE
						  LEFT JOIN (SELECT A.ENTITY_ID,
											A.INT_TBL_CODE_SRL_NUM,
											PEGGED_FLG,
											A.INT_TBL_CODE,
											NRML_PCNT_CR,
											REASON_CODE,
											A.PEG_REVIEW_DATE,
											A.END_DATE,
											A.PEG_FREQUENCY_IN_MONTHS,
											A.PEG_FREQUENCY_IN_DAYS
									   FROM TBAADM.ITC A,
											(SELECT ENTITY_ID,
													MAX(INT_TBL_CODE_SRL_NUM) INT_TBL_CODE_SRL_NUM
											   FROM TBAADM.ITC
											  WHERE ENTITY_TYPE = 'ACCNT'
												AND DEL_FLG = 'N'
												AND ENTITY_CRE_FLG = 'Y'
											  GROUP BY ENTITY_ID) B
									  WHERE A.ENTITY_ID = B.ENTITY_ID
										AND A.INT_TBL_CODE_SRL_NUM = B.INT_TBL_CODE_SRL_NUM) ITC ON GAM.ACID =
																									ITC.ENTITY_ID
						  LEFT JOIN (SELECT ACID,
											ACCT_STATUS,
											ACCT_STATUS_DATE,
											ACCT_CLS_REASON_CODE
									   FROM TBAADM.CAM
									  WHERE DEL_FLG = 'N'
										AND ENTITY_CRE_FLG = 'Y'
									 UNION ALL
									 SELECT ACID,
											ACCT_STATUS,
											ACCT_STATUS_DATE,
											ACCT_CLS_REASON_CODE
									   FROM TBAADM.SMT) CAM ON GAM.ACID = CAM.ACID
						  LEFT JOIN (SELECT DISTINCT REF_CODE, REF_DESC ACCT_CLS_REASON_DESC
									   FROM TBAADM.RCT
									  WHERE REF_REC_TYPE = 'GX') RCT_CAM ON CAM.ACCT_CLS_REASON_CODE =
																			REF_CODE
						  LEFT JOIN TBAADM.GAC ON GAM.ACID = GAC.ACID
						  LEFT JOIN (SELECT DISTINCT SOL_ID, SOL_DESC BRANCH_NAME
									   FROM TBAADM.SOL
									  WHERE DEL_FLG = 'N'
										AND BANK_CODE = '176') SOL ON GAM.SOL_ID = SOL.SOL_ID
						  LEFT JOIN (SELECT REF_CODE, REF_DESC
									   FROM TBAADM.RCT
									  WHERE REF_REC_TYPE = '38') RCT_38 ON RCT_38.REF_CODE =
																		   GAC.NATURE_OF_ADVN
						  LEFT JOIN (SELECT ACID,
											CLOSE_ON_MATURITY_FLG,
											AUTO_RENEWAL_FLG,
											DEPOSIT_PERIOD_MTHS,
											DEPOSIT_PERIOD_DAYS,
											ACCT_STATUS,
											LINK_OPER_ACCOUNT,
											OPEN_EFFECTIVE_DATE,
											MATURITY_DATE,
											MATURITY_AMOUNT,
											DEPOSIT_AMOUNT,
											DEPOSIT_STATUS,
											REPAYMENT_ACID
									   FROM TBAADM.TAM) TAM ON GAM.ACID = TAM.ACID
						  LEFT JOIN (SELECT REF_CODE, REF_DESC
									   FROM TBAADM.RCT
									  WHERE REF_REC_TYPE = 'AF') RCT_AF ON GAC.FREE_CODE_5 =
																		   RCT_AF.REF_CODE
						  LEFT JOIN (SELECT REF_CODE, REF_DESC
									   FROM TBAADM.RCT
									  WHERE REF_REC_TYPE = '21') RCT_OC ON GAC.ACCT_OCCP_CODE =
																		   RCT_OC.REF_CODE
						  LEFT JOIN (SELECT REF_CODE, REF_DESC
									   FROM TBAADM.RCT
									  WHERE REF_REC_TYPE = '27') RCT_27 ON GAM.MODE_OF_OPER_CODE =
																		   RCT_27.REF_CODE
						  LEFT JOIN (SELECT REF_CODE, REF_DESC
									   FROM TBAADM.RCT
									  WHERE REF_REC_TYPE = 'CN') RCT_CN ON GAC.FREE_TEXT_10 =
																		   RCT_CN.REF_CODE
						  LEFT JOIN (SELECT REF_CODE, REF_DESC
									   FROM TBAADM.RCT
									  WHERE REF_REC_TYPE = '31') RCT_31 ON GAM.FREZ_REASON_CODE =
																		   RCT_31.REF_CODE
						  LEFT JOIN (SELECT ENTITY_ID,
											FREE_CODE_20,
											FREE_CODE_22,
											FREE_CODE_21,
											FREE_CODE_5,
											FREE_CODE_6,
											FREE_CODE_1,
											FREE_CODE_43
									   FROM CFCM
									  WHERE CFCM.ENTITY_TYPE = 'A') CFCM ON GAM.FORACID =
																			CFCM.ENTITY_ID
						  LEFT JOIN TBAADM.LA_SAM ON GAM.ACID = LA_SAM.ACID
						  LEFT JOIN (SELECT A.ACID,
											SUM(A.SANCT_LIM) SANCT_LIM,
											MIN(A.LIM_SANCT_DATE) LIM_SANCT_DATE,
											MIN(A.LIM_REVIEW_DATE) LIM_REVIEW_DATE,
											MIN(A.LIM_EXP_DATE) LIM_EXP_DATE,
											MIN(A.APPLICABLE_DATE) APPLICABLE_DATE
									   FROM TBAADM.LHT A
									  WHERE A.STATUS = 'A'
										AND ENTITY_CRE_FLG = 'Y'
										AND DEL_FLG = 'N'
										AND A.applicable_date <= trunc(sysdate)-1
									  GROUP BY ACID ) LHT ON GAM.ACID = LHT.ACID
						  LEFT JOIN (SELECT ACID,
											REP_PERD_MTHS,
											REP_PERD_DAYS,
											OP_ACID,
											PAYOFF_FLG,
											ACCT_STATUS_FLG,
											EI_PERD_START_DATE,
											PAYOFF_DATE,
											EI_PERD_END_DATE,
											PAYOFF_REASON_CODE,
											CRFILE_REF_ID,
											CASE
											  WHEN EI_SCHM_FLG = 'Y' THEN
											   'EI'
											  ELSE
											   'NON_EI'
											END AS EMI_TYPE,
											DMD_SATISFY_MTHD AS PAYMENT_METHOD
									   FROM TBAADM.LAM) LAM ON GAM.ACID = LAM.ACID
						  LEFT JOIN (SELECT REF_CODE, REF_DESC
									   FROM TBAADM.RCT
									  WHERE REF_REC_TYPE = 'EY') RCT_EY ON LAM.PAYOFF_REASON_CODE =
																		   RCT_EY.REF_CODE
						  LEFT JOIN (SELECT B2K_ID, MAIN_CLASSIFICATION_USER FROM TBAADM.ACD) ACD ON GAM.ACID =
																									 ACD.B2K_ID
						  LEFT JOIN (SELECT FORACID, DEBIT_CARD FROM C_DBC) C_DBC ON GAM.FORACID =
																					 C_DBC.FORACID
						  LEFT JOIN (SELECT LIMIT_B2KID, LIM_EXP_DATE, LIMIT_REVIEW_DATE
									   FROM TBAADM.LLT) LLT ON GAM.LIMIT_B2KID = LLT.LIMIT_B2KID
						  LEFT JOIN (SELECT REF_CODE, REF_DESC
									   FROM TBAADM.RCT
									  WHERE REF_REC_TYPE = 'AI') RCT_AI ON GAC.FREE_CODE_8 =
																		   RCT_AI.REF_CODE
						  LEFT JOIN (SELECT ACID,
											TO_CHAR(MAX(CHRGE_OFF_DATE), 'YYYY-MM-DD') CHRGE_OFF_DATE
									   FROM (SELECT ACID, CHRGE_OFF_PRINCIPAL, CHRGE_OFF_DATE
											   FROM TBAADM.TA_COT
											  WHERE ENTITY_CRE_FLG = 'Y'
												AND DEL_FLG = 'N'
											 UNION ALL
											 SELECT ACID, CHRGE_OFF_PRINCIPAL, CHRGE_OFF_DATE
											   FROM TBAADM.COT
											  WHERE ENTITY_CRE_FLG = 'Y'
												AND DEL_FLG = 'N') T
									  WHERE T.CHRGE_OFF_DATE IS NOT NULL
									  GROUP BY ACID) V ON GAM.ACID = V.ACID
						  LEFT JOIN (SELECT ENTITY_ID, NEXT_PEG_REVIEW_DATE
									   FROM TBAADM.EIT
									  WHERE NEXT_PEG_REVIEW_DATE IS NOT NULL) PR ON GAM.ACID =
																					PR.ENTITY_ID
						  LEFT JOIN (SELECT CUSTID,
											CASE
											  WHEN Z1.TAG = '1' THEN
											   'APPROVED'
											  WHEN Z1.TAG = '3' THEN
											   'NOT APPROVED'
											  ELSE
											   'NOT LINKED'
											END SIGNATURE_STATUS
									   FROM (SELECT ROW_NUMBER() OVER(PARTITION BY CUSTID ORDER BY CUSTID, MODIFIEDDATE DESC) SRNO,
													Z.*
											   FROM (SELECT CUSTID,
															'1' AS TAG,
															MODIFIEDDATE 
													   FROM SVSUSER.SIGNCUSTINFO
													  WHERE TRIM(CUSTID) IS NOT NULL
													 UNION ALL
													 SELECT CUSTID,
															'3' AS TAG,
															 MODIFIEDDATE
													   FROM SVSUSER.NSIGNCUSTINFO
													  WHERE TRIM(CUSTID) IS NOT NULL) Z) Z1
									  WHERE SRNO = 1) S ON GAM.CIF_ID = S.CUSTID
									  
									  
		EDWH
		C.
			UPDATE ADF_CLIENT.ACCOUNT_MASTER A
			FROM ADF_CDR.ACCOUNT_MASTER B
			SET a.initial_funding = b.initial_funding,
			a.last_credit_tran_date = b.last_credit_tran_date,
			a.last_debit_tran_date = b.last_debit_tran_date,
			a.ib_last_tran_date = b.ib_last_tran_date,
			a.atm_last_tran_date = b.atm_last_tran_date,
			a.opening_tran_date = b.opening_tran_date,
			a.lr_freq_type = b.lr_freq_type,
			a.emiamt = b.emiamt
			WHERE A.ACCOUNT_NO = B.ACCOUNT_NO
			
			
		D.
			UPDATE ADF_CLIENT.ACCOUNT_MASTER A
			FROM ADF_CLIENT.RKIT_ACCT B
			SET A.OPENING_TRAN_DATE = B.RCRE_TIME,
			A.OPENING_DATE = B.ACCT_OPN_DATE,
			a.account_holder_name=b.acct_name,
			A.closing_Date = b.ACCT_CLS_DATE
			WHERE A.ACCOUNT_NO = B.FORACID
		E.
			update adf_client.account_master a
			from (SELECT row_number() over(partition by acid order by acid,next_emi_date,lr_freq_type ) srno,* FROM ADF_CDR.LRS) LRS
			SET A.EMIAMT = LRS.EMIAMT,
			A.lr_freq_type = LRS.lr_freq_type,
			A.NEXT_EMI_DATE = LRS.NEXT_EMI_DATE
			WHERE A.ACID = LRS.ACID and lrs.srno=1
		F.
			--CALL VECTORWISE (COMBINE 'adf_cdr.account_master - adf_cdr.account_master');

			DELETE FROM ADF_CDR.ACCOUNT_MASTER WHERE ACCOUNT_NO IN (SELECT DISTINCT ACCOUNT_NO FROM ADF_CLIENT.ACCOUNT_MASTER);

			insert into  adf_cdr.account_master(gl_code,gl_sub_head_code,account_no,account_holder_name,customer_code,opening_date,product_code,currency_code,closing_date,line_id,branch_code,dormant_date,
			account_type,account_prefix,psl_code,psl_desc,maturity_amount,amount,booking_date,maturity_date,interest_rate,ref_code,ref_desc,advance_type,advance_purpose,pp20_code,
			pp20_desc,roi_type,borrower_code,occupation_code,occupation_desc,bsr3_commodity_code,subsidy,initial_sanction_limit,initial_sanction_date,investment_flag,
			last_credit_tran_date,last_debit_tran_date,loan_end_date,opening_tran_date,acct_business_segment,initial_funding,religion_code,religion,booking_date_1,
			live_closed,deposit_status,scheme_description,rm,mode_of_operation,mode_of_operation_desc,bar_code,asset_classification,promo_code,debit_card,npa_flg,npa_date,branch_name,
			lc_code,lg_code,business_seg_desc,account_ownership,cheq_allowed,freeze_code,freeze_desc,freeze_reason_code,freeze_reason_desc,risk_code,enterer_id,inactive_date,
			ib_last_tran_date,atm_last_tran_date,loan_payoff_date,reason_for_closure,emiamt,AQB_WAIVER,RATE_CODE,EMI_START_DATE,acid,link_oper_account,account_status,
			gov_scheme_code,ROI_CARD_RATE,payoff_flg,lr_freq_type,App_Ref_No,prefered_account,chrg_off_date,cc_renewal_date,ptc_flag,group_code,urn_no,renewal_flag,charge_off_flg,deposit_period_mths,deposit_period_days,DSB_FLAG,
			Promo_Code_N,Risk_category_code,RISK_DESC,customernreflg,LIMIT_REVIEW_DATE,LIMIT_expiry_DATE,
			DRAWING_POWER,DRAWING_POWER_IND,LCHG_USER_ID,RCRE_USER_ID,SCHEME_TYPE,INDUSTRY_TYPE,next_emi_date,linked_sb_acid,source_deal_code,disbursal_deal_code,closure_flag,
			LAST_FREZ_DATE,ACCT_STATUS_DATE,last_tran_date,peg_review_date,APPLICABLE_DATE,ACCT_LVL_GROUPING,aadhar_seeding_YN,project_finance_YN,chrg_level_code,
			SIGNATURE_STATUS,EMI_TYPE,PAYMENT_METHOD,repricing_plan,repayment_acid)
			SELECT gl_code,gl_sub_head_code,account_no,account_holder_name,customer_code,opening_date,product_code,currency_code,closing_date,line_id,branch_code,dormant_date,
			account_type,account_prefix,psl_code,psl_desc,maturity_amount,amount,booking_date,maturity_date,interest_rate,ref_code,ref_desc,advance_type,advance_purpose,pp20_code,
			pp20_desc,roi_type,borrower_code,occupation_code,occupation_desc,bsr3_commodity_code,subsidy,initial_sanction_limit,initial_sanction_date,investment_flag,
			last_credit_tran_date,last_debit_tran_date,loan_end_date,opening_tran_date,acct_business_segment,initial_funding,religion_code,religion,booking_date_1,
			live_closed,deposit_status,scheme_description,rm,mode_of_operation,mode_of_operation_desc,bar_code,asset_classification,promo_code,debit_card,npa_flg,npa_date,branch_name,
			lc_code,lg_code,business_seg_desc,account_ownership,cheq_allowed,freeze_code,freeze_desc,freeze_reason_code,freeze_reason_desc,risk_code,enterer_id,inactive_date,
			ib_last_tran_date,atm_last_tran_date,loan_payoff_date,reason_for_closure,emiamt,AQB_WAIVER,RATE_CODE,EMI_START_DATE,acid,link_oper_account,account_status,gov_scheme_code,ROI_CARD_RATE,payoff_flg,lr_freq_type,App_Ref_No,
			prefered_account,chrge_off_date,cc_renewal_date,ptc_flag,group_code,urn_no,renewal_flag,charge_off_flg,deposit_period_mths,deposit_period_days,DSB_FLAG,Promo_Code_N,Risk_category_code,RISK_DESC,customernreflg,LIMIT_REVIEW_DATE,LIMIT_expiry_DATE,
			drwng_power,drwng_power_ind,LCHG_USER_ID,RCRE_USER_ID,SCHM_TYPE,INDUSTRY_TYPE,next_emi_date,linked_sb_acid,source_deal_code,disburse_deal_code,closure_flag,
			LAST_FREZ_DATE,ACCT_STATUS_DATE,last_tran_date,peg_review_date,APPLICABLE_DATE,ACCT_LVL_GROUPING,aadhar_seeding_YN,project_finance_YN,chrg_level_code,
			SIGNATURE_STATUS,EMI_TYPE,PAYMENT_METHOD,repricing_plan,repayment_acid
			FROM adf_client.account_master;

			update adf_cdr.account_master set interest_reset_date = (select case when b.reason_code<>'RESET' then null 
			when b.reason_code = 'RESET' and a.interest_reset_date is null then current_date-1 else a.interest_reset_date end as interset_reset_date
			from adf_cdr.account_master a, adf_client.account_master b  where a.account_no=b.account_no);

			update adf_cdr.account_master a set a.interest_rate=(select case when b.credit_rate_of_interest>0 then b.credit_rate_of_interest else b.debit_rate_of_interest end from adf_cdr.all_acctbal_intrate b 
			where a.account_no=b.account_no  and b.rate_date='${Report_Date}');


			CALL VECTORWISE (COMBINE 'adf_client.account_master - adf_client.account_master');


		G.
			UPDATE ADF_CDR.ACCOUNT_MASTER A
			FROM ADF_CLIENT.RKIT_ACCT B
			SET A.OPENING_TRAN_DATE = B.RCRE_TIME,
			A.OPENING_DATE = B.ACCT_OPN_DATE,
			a.account_holder_name=b.acct_name
			WHERE A.ACCOUNT_NO = B.FORACID
		H.
			update adf_cdr.account_master set aadhar_seeding_yn = Null;

			update adf_cdr.account_master am from
			(select *
			from   bsg_read.cfcm
			where  entity_type='A') x
			set   am.lc_code=free_code_5,
				   am.lg_code=free_code_6,
				   am.aadhar_seeding_yn=left(nvl(free_code_43,'N'),1),
				  am.BSR3_COMMODITY_CODE= x.FREE_CODE_1,
			   am.Prefered_Account=x.FREE_CODE_21 ,
			am.DSB_FLAG =x.FREE_CODE_20,
			   Promo_Code_N = x.free_code_22  
			 where am.account_no=x.entity_id;
		I.
			update adf_cdr.account_master a
			from (
			SELECT h.account_no,
				  MAX(CASE
				  WHEN H.PART_TRAN_TYPE = 'C' THEN
				  H.TRAN_DATE
				  END) LAST_CREDIT_TRAN_DATE,
				  MAX(CASE
				  WHEN H.PART_TRAN_TYPE = 'D' THEN
				  H.TRAN_DATE
				  END) LAST_DEBIT_TRAN_DATE,
				  MAX(CASE WHEN TRAN_TYPE='T' AND TRAN_SUB_TYPE='CI' AND H.RCRE_USER_ID IN ('FIVUSR','NEFT') THEN   TRAN_DATE
				  ELSE NULL END)  IB_LAST_TRAN_DATE,
				  MAX(CASE WHEN TRAN_TYPE='T' AND TRAN_SUB_TYPE='CI'  AND H.RCRE_USER_ID = 'CDCI' THEN   TRAN_DATE
				  ELSE NULL
				  END)  ATM_LAST_TRAN_DATE         
				  FROM ADF_CDR.vw_HTD H
				  WHERE --H.TRAN_DATE >= current_date-2 AND
				  H.PSTD_FLG = 'Y' and h.cust_bank_induced <>'BANK_INDUCED' 
				  GROUP BY h.account_no)b
			set a.LAST_CREDIT_TRAN_DATE = b.LAST_CREDIT_TRAN_DATE,
			a.LAST_DEBIT_TRAN_DATE = b.LAST_DEBIT_TRAN_DATE,
			A.IB_LAST_TRAN_DATE = B.IB_LAST_TRAN_DATE,
			A.ATM_LAST_TRAN_DATE = B.ATM_LAST_TRAN_DATE
			where a.account_no = b.account_no and
				  a.closing_date is null and
				  a.scheme_type in ('ODA','SBA','LAA','CLA','PCA','FBA');
				  
		J.
			update adf_cdr.account_master x from
			(SELECT a.account_no,sum(tran_amt * exchange_rate) ip_value
			FROM   adf_cdr.vw_HTD H, adf_cdr.account_master a
			WHERE  a.opening_tran_date>='01-Jan-2015'           and
					a.account_prefix IN ('30','40','60') and
					a.account_no   = H.account_no                  AND
					--H.VALUE_DATE  >= a.opening_date                AND
					H.VALUE_DATE  <= a.opening_tran_date           AND
					H.PART_TRAN_TYPE='C' AND 
					H.PSTD_FLG = 'Y' 
			 group by a.account_no ) h
			set x.initial_funding=h.ip_value 
			 WHERE  x.account_no = H.account_no;

			update adf_cdr.account_master x from
			(SELECT a.account_no,sum(tran_amt * exchange_rate) ip_value
			FROM   adf_cdr.vw_HTD H, adf_cdr.account_master a
			WHERE  a.opening_tran_date>='01-Jan-2015'     and
					a.account_prefix IN ('70','75') and
					a.account_no   = H.account_no                  AND
					--H.VALUE_DATE  >= a.booking_date                AND
					H.VALUE_DATE  <= a.opening_tran_date           AND
					H.PART_TRAN_TYPE='C' AND 
					H.PSTD_FLG = 'Y' 
			 group by a.account_no ) h
			set x.initial_funding=h.ip_value 
			 WHERE  x.account_no = H.account_no;

		K.
			update ADF_CDR.ACCOUNT_MASTER set nomination = 'N';

			UPDATE ADF_CDR.ACCOUNT_MASTER A
			FROM (select distinct acid from bsg_read.nomination_details) B
			SET A.nomination = 'Y'
			WHERE A.ACID = B.ACID
			
		L.
			update adf_cdr.customer_master cm
			from 
				(select z1.customer_code,z1.create_date,z1.opening_tran_date 
				 from (select z.*, x.dt 
					   from (select  cm.customer_code,cm.create_date, am.opening_tran_date 
						   from   adf_cdr.customer_master cm
							  inner join (select  am.customer_code,min(am.opening_tran_date) opening_tran_date
										 from adf_cdr.account_master am 
										  inner join bsg_read.rbl_instakit r on am.account_no=r.account_number
										  group by am.customer_code) am on cm.customer_code = am.customer_code) z
				   left  join (select customer_code,min(opening_tran_date) dt from adf_cdr.account_master group by customer_code) x on z.customer_code=x.customer_code and z.opening_tran_date>x.dt) z1
				 where  z1.dt is null)a
			set cm.create_date = a.opening_tran_date
			where cm.customer_code = a.customer_code
		
		M.delete from bsg_read.cust_schm_transfer where transfer_date = CURRENT_DATE-1;
		
		N.SELECT g.foracid,source_schm,target_schm,to_char(s.rcre_time,'yyyy-mm-dd') dt
       FROM   tbaadm.sct s
              INNER JOIN tbaadm.gam g ON s.acid=g.acid
        WHERE s.entity_cre_flg='Y' AND s.del_flg='N'
and trunc(s.rcre_time) = (select dc_cls_date from tbaadm.gct )

O.

select g.foracid account_no,g.acct_cls_date
FROM  tbaadm.gam G 
inner JOIN tbaadm.alr alr ON G.acid = alr.acid AND alr.acct_label like '%INSTAKIT%'
INNER JOIN tbaadm.GAC on g.acid =gac.acid
where g.entity_cre_flg = 'Y'
and g.del_flg = 'N'
AND (ACCT_CLS_DATE IS NOT NULL OR FREE_TEXT_8 IS NOT NULL)

P.
update adf_cdr.account_master a
from
(
select account_number,acct_cls_date from  bsg_read.rkit_account ) b
set a.closing_date = b.acct_cls_date
where a.account_no = b.account_number

Q.

sELECT distinct replace(replace(card_type,'0,',''),',','') card_type,replace(replace(card_sub_type,'0,',''),',','')card_sub_type,
FORACID Account_no
FROM custom.C_TBL_CARD_TYPE_DETAILS 
WHERE ENTITY_CRE_FLG='Y' and DEL_FLG='N'
and trunc(lchg_time)>=trunc(sysdate)-30

S.

update adf_cdr.account_master A
from (SELECT * FROM adf_client.account_master)B
set a.card_type = b.card_type ,
	a.card_sub_type = b.card_sub_type
WHERE A.ACCOUNT_NO = B.ACCOUNT_NO







2.Customer Master

FINMIS
A.

			SELECT ACCOUNTS.ORGKEY CUSTOMER_CODE,
				   TO_DATE(TO_CHAR(ACCOUNTS.BODATECREATED, 'DD-Mon-YYYY'), 'DD-Mon-YYYY') CREATE_DATE,
				   ACCOUNTS.NAME CUSTOMER_NAME,
				   GENDER,
				   CASE
					 WHEN ACCOUNTS.CORP_ID IS NULL THEN
					  'I'
					 ELSE
					  'C'
				   END CUSTOMER_TYPE,
				   'FINACLE' CUSTOMER_SOURCE,
				   ACCOUNTS.STRUSERFIELD5,
				   TRADEFINANCE.STR6 IEC,
				   CASE
					 WHEN ACCOUNTS.CONSTITUTION_CODE IN ('C006', 'C010', 'C024', 'C033') THEN
					  'Y'
					 ELSE
					  'N'
				   END PSU_FLAG,
				   CASE
					 WHEN ACCOUNTS.CORP_ID IS NULL THEN
					  DEMOGRAPHIC.USERFIELD5
					 ELSE
					  CORPORATE.STRUSERFIELD12
				   END BSR_ORG_CODE,
				   ACCOUNTS.CONSTITUTION_CODE,
				   NVL(CFCM.FREE_CODE_11, GAC.BORROWER_CATEGORY_CODE) BORROWER_CODES,
				   nvl(corporate.STRUSERFIELD15,ACCOUNTS.STRFIELD19) CASTE,
				   ACCOUNTS.CONSTITUTION_DESC,
				   COREINTERFACE.FREE_TEXT_5 MALE_SHG,
				   COREINTERFACE.FREE_TEXT_6 FEMALE_SHG,
				   nvl(corporate.STRUSERFIELD26,ACCOUNTS.WEAKER_SECTION) weaker_section,
				   ACCOUNTS.CUST_SP_CATEGORY,
				   ACCOUNTS.SEGMENTATION_CLASS SEGMENT,
				   ACCOUNTS.SUBSEGMENT,
				   CASE
					 WHEN ACCOUNTS.CORP_ID IS NULL THEN
					  DEMOGRAPHIC.USERFIELD6
					 ELSE
					  CORPORATE.STRUSERFIELD13
				   END BUSINESS_SEGMENT,
				   CASE
					 WHEN ACCOUNTS.CORP_ID IS NULL THEN
					  (SELECT RCT.REF_DESC
						 FROM TBAADM.RCT
						WHERE RCT.REF_REC_TYPE = 'CN'
						  AND REF_CODE = DEMOGRAPHIC.USERFIELD6
						  AND ROWNUM < 2)
					 ELSE
					  (SELECT RCT.REF_DESC
						 FROM TBAADM.RCT
						WHERE RCT.REF_REC_TYPE = 'CN'
						  AND REF_CODE = CORPORATE.STRUSERFIELD13
						  AND ROWNUM < 2)
				   END BUSINESS_SEGMENT_DESC,
				  nvl(corporate.STRUSERFIELD20,ACCOUNTS.STRUSERFIELD5 ) RELIGION_CODE,
				   ACCOUNTS.RELIGION,
				   ACCOUNTS.PHONE_HOME,
				   ACCOUNTS.PHONE_HOME2,
				   ACCOUNTS.PHONE_CELL,
				   CASE WHEN accounts.corp_id IS NULL THEN ACCOUNTS.CUST_DOB
						ELSE CORPORATE.DATE_OF_INCORPORATION
				   END  CUST_DOB,
				   ACCOUNTS.EMAIL_HOME,
				   ACCOUNTS.EMAIL_PALM,
				   ACCOUNTS.EMAIL,
				   ACCOUNTS.STRFIELD16 PAN,
				   ACCOUNTS.ADDRESS_LINE1,
				   ACCOUNTS.ADDRESS_LINE2,
				   ACCOUNTS.ADDRESS_LINE3,
				   ACCOUNTS.ZIP,
				   CFCM.FREE_CODE_13 MODEL_RATING,
				   CFCM.FREE_CODE_14 MASTER_RATING,
				   CFCM.FREE_CODE_10 INDUSTRY_CODE,
				   CASE WHEN ACCOUNTS.CORP_ID IS NULL THEN ACCOUNTS.MANAGER
					  ELSE NVL(CORPORATE.PRIMARYRMLOGIN_ID,ACCOUNTS.MANAGER)
				  END RM_NAME,
				   ACCOUNTS.STAFFEMPLOYEEID STAFF_EMP_NO,
				   nvl(ACCOUNTS.DONOTCALLFLAG,corporate.STR5) DND_FLAG,
				   MISC.EMPLOYERID COMPANY_CODE,
				   ID.REF_DESC INDUSTRY_DESC,
				   ACCOUNTS.ISEBANKINGENABLED INTERNET_BANKING,
				   ACCOUNTS.ISSMSBANKINGENABLED SMS_BANKING,
				   ACCOUNTS.USERFLAG1 DEBIT_CARD,
				   null CARD_TYPE,
				   ACCOUNTS.SALUTATION,
				   ACCOUNTS.CUST_FIRST_NAME FIRST_NAME,
				   ACCOUNTS.CUST_MIDDLE_NAME MIDDLE_NAME,
				   ACCOUNTS.CUST_LAST_NAME LAST_NAME,
				   ACCOUNTS.NAME FULL_NAME,
				   --ACCOUNTS.OCCUPATION,
				   oc.OCC_CD OCCUPATION,
				   ACCOUNTS.CIF_CREATION_FLAG,       
				   DR.REFERENCENUMBER DRIVING_LICENSE,       
				   UI.REFERENCENUMBER AS "UID",
				   VOT.REFERENCENUMBER VOTER_ID,       
				   PAS.REFERENCENUMBER PASSPORT_NO,       
				   RAT.REFERENCENUMBER RATION_CARD_NO,
				   ACCOUNTS.STRFIELD20 AS SPECIALCATEGORY,
				   ACCOUNTS.CUSTOMERMINOR AS MINOR,
					   PRIMARY_SOL_ID AS BRANCH_CODE,
					   BR.SOL_DESC AS BRANCH_NAME,
					   BOR.ref_desc  Borrower_Description,
			CASE WHEN ACCOUNTS.CORP_ID IS NULL THEN ACCOUNTS.SECONDARYRM_ID
					  ELSE NVL(CORPORATE.SECONDRMLOGIN_ID,ACCOUNTS.SECONDARYRM_ID)
				  END  SECONDARY_RM,
			Case when NVL(GM.CNT,0)>0 then 'Y'
			Else 'N'
			End   KYC_FLG,
			NULL SECTOR_DESC,
			ACCOUNTS.CUSTOMERNREFLG,CUST.CUSTOMER_TYPE_CODE,CUST.CUSTOMER_TYPE_DESC,
			DEMOGRAPHIC.NATIONALITY,X.REFERENCENUMBER AADHAR_NUMBER,
			DEMOGRAPHIC.TDSEXCEMPTREMARKS EDUCATION_QUALIFICATION,
			DEMOGRAPHIC.EMPLOYMENT_STATUS,
			nvl(J.LOCALETEXT,Jd.LOCALETEXT) GROSS_ANNUAL_INCOME,
			NVL(ACCOUNTS.StrUserfield20,CORPORATE.StrUserfield8) UCIC,
			DEMOGRAPHIC.MARITAL_STATUS,accounts.preferredname,accounts.MAIDENNAMEOFMOTHER as maidenname,
			fin_industry_code,fin_industry_DESC,ACCOUNTS.STAFFFLAG,ACCOUNTS.ISEBANKINGENABLED INTERNET_BANKING_FLAG,
			ACCOUNTS.ISSMSBANKINGENABLED SMS_BANKING_FLAG,ACCOUNTS.USERFLAG4 PHONE_BANKING_FLAG,
			CAST(ACCOUNTS.STRFIELD7 AS VARCHAR2(100)) CARD_TYPE,
				 CAST(ACCOUNTS.STRFIELD8 AS VARCHAR2(100)) CARD_SUBTYPE,
			(CASE WHEN TRIM(NVL(ACCOUNTS.USERFLAG1,' '))='Y' THEN 'Y'
					 ELSE 'N'
				 END) DEBIT_FLAG,ACCOUNTS.RATING CUST_RATING_CODE,ACCOUNTS.RATINGDATE CUST_RATING_DATE,
				 ACCOUNTS.STRFIELD18 SOURCING_ID,CUST_ID,
				   case when accounts.corp_id is null then ACCOUNTS.KYC_REMEDIATION_DATE else corporate.date3 END  KYC_REMEDIATION_DATE,
					  str.attrribute_desc as Employment_type,
					  CAPACITY_OF_ENTITY.CAPACITY_OF_ENTITY,
					  cmg_trade.trade_flg,
			accounts.Lead_Source                  
			  FROM  (SELECT ACCOUNTS.*,CASE
					 WHEN ACCOUNTS.BODATECREATED > '15-Dec-2012' THEN       'Post-Migration'         ELSE          'Pre-Migration'
				   END AS CIF_CREATION_FLAG,CASE
					 WHEN ACCOUNTS.STRUSERFIELD5 = 'MIN10' THEN
					  'MIN10-Christians'
					 WHEN ACCOUNTS.STRUSERFIELD5 = 'MIN20' THEN
					  'MIN20-Muslims'
					 WHEN ACCOUNTS.STRUSERFIELD5 = 'MIN30' THEN
					  'MIN30-Buddhists'
					 WHEN ACCOUNTS.STRUSERFIELD5 = 'MIN40' THEN
					  'MIN40-Sikhs'
					 WHEN ACCOUNTS.STRUSERFIELD5 = 'MIN50' THEN
					  'MIN50-Zoroastrians'
					 WHEN ACCOUNTS.STRUSERFIELD5 = 'MIN60' THEN
					  'MIN60-Other Minority'
					 WHEN ACCOUNTS.STRUSERFIELD5 = 'MIN90' OR
						  ACCOUNTS.STRUSERFIELD5 IS NULL THEN
					  'Others Non Minority'
				   END RELIGION,userflag2 DONOTCALLFLAG,
								CL.LOCALETEXT CONSTITUTION_DESC,
								CL1.LOCALETEXT WEAKER_SECTION,
								CL2.LOCALETEXT CUST_SP_CATEGORY,
					  ACCOUNTS.DTDATE1 KYC_REMEDIATION_DATE
						   FROM CRMUSER.ACCOUNTS
						   LEFT JOIN (SELECT DISTINCT A.VALUE, B.LOCALETEXT
									   FROM CRMUSER.CATEGORIES A
									   LEFT JOIN CRMUSER.CATEGORY_LANG B 
									   ON B.CATEGORYID = A.CATEGORYID
									  WHERE CATEGORYTYPE = 'RB_CONSTITUTION') CL 
									  ON CL.VALUE = ACCOUNTS.CONSTITUTION_CODE
						   LEFT JOIN (SELECT A.VALUE, B.LOCALETEXT
									   FROM CRMUSER.CATEGORIES A,
											CRMUSER.CATEGORY_LANG B,
											(SELECT DISTINCT STRUSERFIELD6
											   FROM CRMUSER.ACCOUNTS) C
									  WHERE A.CATEGORYID = B.CATEGORYID
										AND CATEGORYTYPE = 'RB_WEAKER_SECTION'
										AND C.STRUSERFIELD6 = A.VALUE) CL1 
										ON CL1.VALUE = ACCOUNTS.STRUSERFIELD6
						   LEFT JOIN (SELECT C.VALUE, CL.LOCALETEXT
									   FROM CRMUSER.CATEGORY_LANG CL,
											CRMUSER.CATEGORIES    C
									  WHERE C.VALUE IN
											('SC001', 'SC002', 'SC003', 'SC004', 'SC005')
										AND C.CATEGORYID = CL.CATEGORYID) CL2 
										ON CL1.VALUE = ACCOUNTS.STRFIELD20) ACCOUNTS           
			  LEFT JOIN CRMUSER.CORPORATE ON ACCOUNTS.ORGKEY = CORPORATE.CORP_KEY
			  LEFT JOIN (SELECT DISTINCT CUST_ID,CIF_ID FROM TBAADM.CMG WHERE ENTITY_CRE_FLG = 'Y' AND DEL_FLG = 'N') CMG
			  ON ACCOUNTS.ORGKEY = CMG.CIF_ID
			  LEFT JOIN (SELECT ORGKEY,USERFIELD5,USERFIELD6,DONOTCALLFLAG,NATIONALITY,
			  TDSEXCEMPTREMARKS,EMPLOYMENT_STATUS,MARITAL_STATUS FROM CRMUSER.DEMOGRAPHIC WHERE 
			  TMDATE = (SELECT MAX(TMDATE) FROM CRMUSER.DEMOGRAPHIC DEMO WHERE DEMOGRAPHIC.ORGKEY = DEMO.ORGKEY)) DEMOGRAPHIC
			  ON ACCOUNTS.ORGKEY = DEMOGRAPHIC.ORGKEY
			  LEFT JOIN CFCM ON ACCOUNTS.ORGKEY = CFCM.ENTITY_ID
							AND CFCM.ENTITY_TYPE = 'C'
			  LEFT JOIN (SELECT MAX(BORROWER_CATEGORY_CODE) BORROWER_CATEGORY_CODE,
								GAM.CIF_ID
						   FROM TBAADM.GAC, TBAADM.GAM
						  WHERE GAM.ACID = GAC.ACID
							AND BORROWER_CATEGORY_CODE IS NOT NULL
						  GROUP BY CIF_ID) GAC ON GAC.CIF_ID = ACCOUNTS.ORGKEY
			   LEFT JOIN CRMUSER.COREINTERFACE ON COREINTERFACE.ORGKEY = ACCOUNTS.ORGKEY
			   LEFT JOIN (select distinct orgkey,STR6 from CRMUSER.TRADEFINANCE) tradefinance ON TRADEFINANCE.ORGKEY = ACCOUNTS.ORGKEY
			   LEFT JOIN (select distinct orgkey, employerid, STRtext27,STRtext22 from CRMUSER.MISCELLANEOUSINFO where upper(TYPE) ='CURRENT_EMPLOYMENT') MISC ON ACCOUNTS.ORGKEY = MISC.ORGKEY
			  left join (select to_char(a.categoryid) attrribute_id,to_char(value) attrribute_code,to_char(localetext) attrribute_desc,categorytype attrribute_type,'CRM' tag 
			from crmuser.categories a, CRMUSER.category_lang b where a.categoryid=b.categoryid)  str on   misc.STRtext22 = str.attrribute_code and str.attrribute_type = 'RB_FIRM_TYPE'
			   LEFT JOIN 
			(select H.VALUE,A.LOCALETEXT from crmuser.category_lang a INNER JOIN 
			(select DISTINCT VALUE,CATEGORYID from crmuser.categories v where v.categorytype = 'RB_ANNUAL_INCOME') H
			ON a.categoryid = H.categoryid)JD
			ON misc.STRtext27 = JD.VALUE
			   LEFT JOIN (select CIF_ID,count(1) CNT from tbaadm.gam where frez_REASON_code in ('9', '10', '40') AND acct_cls_date is null and gam.entity_cre_flg='Y' and gam.del_flg='N' GROUP BY CIF_ID) GM ON ACCOUNTS.orgkey = gm.cif_id
			LEFT JOIN (SELECT A.ORGKEY,C.VALUE CUSTOMER_TYPE_CODE, CL.LOCALETEXT CUSTOMER_TYPE_DESC
			FROM CRMUSER.CATEGORY_LANG CL,CRMUSER.CATEGORIES C,
			(SELECT ORGKEY,CUST_TYPE_CODE FROM CRMUSER.ACCOUNTS) A
			WHERE C.VALUE = A.CUST_TYPE_CODE AND C.CATEGORYID = CL.CATEGORYID AND categorytype='RB_CUST_TYPE') CUST
			ON ACCOUNTS.ORGKEY = CUST.ORGKEY
			LEFT JOIN (SELECT * 
					   FROM (SELECT row_number() over(PARTITION BY orgkey ORDER BY tmdate DESC) srno,ORGKEY,REFERENCENUMBER  
					 FROM CRMUSER.ENTITYDOCUMENT X 
					 WHERE X.DOCCODE = 'UID' 
					/* AND IDENTIFICATIONTYPE = 'SSN'*/ 
					AND length(nvl(referencenumber, ' '))=12) q WHERE  q.srno=1) X ON ACCOUNTS.ORGKEY = X.ORGKEY
			LEFT JOIN (SELECT DISTINCT SOL_ID,SOL_DESC FROM TBAADM.SOL WHERE DEL_FLG='N' AND BANK_CODE='176') BR
			ON ACCOUNTS.PRIMARY_SOL_ID = BR.SOL_ID
			LEFT JOIN (Select DISTINCT REF_CODE,ref_desc from tbaadm.lrct where ref_rec_type='CC11' 
			and del_flg='N' ) BOR
			ON NVL(CFCM.FREE_CODE_11,GAC.BORROWER_CATEGORY_CODE) = BOR.REF_CODE
			LEFT JOIN (SELECT DISTINCT REF_CODE,REF_DESC FROM TBAADM.LRCT 
							   WHERE REF_REC_TYPE='CC10' AND DEL_FLG='N') ID
					   ON CFCM.FREE_CODE_10 = ID.REF_CODE
			LEFT JOIN (SELECT DISTINCT ORGKEY,REFERENCENUMBER
					  FROM CRMUSER.ENTITYDOCUMENT E
					 WHERE DOCCODE = 'DL' 
					   AND ROWNUM < 2) DR
					   ON ACCOUNTS.ORGKEY = DR.ORGKEY
			LEFT JOIN (SELECT ORGKEY,REFERENCENUMBER
					  FROM CRMUSER.ENTITYDOCUMENT
					 WHERE DOCCODE = 'UID'
					   AND ROWNUM < 2) UI
			ON ACCOUNTS.ORGKEY = UI.ORGKEY
			LEFT JOIN (SELECT ORGKEY,REFERENCENUMBER
					  FROM CRMUSER.ENTITYDOCUMENT E
					 WHERE DOCCODE = 'VID'
					   AND ROWNUM < 2) VOT
					   ON ACCOUNTS.ORGKEY = VOT.ORGKEY
			LEFT JOIN (SELECT ORGKEY,REFERENCENUMBER
					  FROM CRMUSER.ENTITYDOCUMENT E
					 WHERE DOCCODE = 'PP'
					   AND ROWNUM < 2) PAS
					   ON ACCOUNTS.ORGKEY = PAS.ORGKEY
			LEFT JOIN (SELECT ORGKEY,REFERENCENUMBER
					  FROM CRMUSER.ENTITYDOCUMENT E
					 WHERE DOCCODE = 'RC'
					   AND ROWNUM < 2) RAT
					   ON ACCOUNTS.ORGKEY = RAT.ORGKEY
			left join (select orgkey,STRTEXT2 OCC_CD
					   from   crmuser.miscellaneousinfo
					   WHERE  TYPE='CURRENT_EMPLOYMENT' AND
							  ENTITY_CRE_FLG='Y') oc on ACCOUNTS.ORGKEY = oc.orgkey
			LEFT JOIN (
			select orgkey,STRUSERFIELD7,b.localetext as CAPACITY_OF_ENTITY from crmuser.accounts a left join 
			(select c.value,cl.localetext from crmuser.categories c inner join crmuser.category_lang cl on c.categoryid=cl.categoryid  and c.categorytype='RB_CAPACITY_ENTITY')
			b on a.STRUSERFIELD7=b.value)  CAPACITY_OF_ENTITY ON ACCOUNTS.ORGKEY=CAPACITY_OF_ENTITY.ORGKEY

			LEFT JOIN 
			(SELECT cif_id,nvl(party_flg,'N') trade_flg
			from   tbaadm.cmg
			where 
				   entity_cre_flg='Y' and
				   del_flg='N'
			) CMG_TRADE ON ACCOUNTS.ORGKEY=CMG_TRADE.CIF_ID           
					   LEFT JOIN 
			(select H.VALUE,A.LOCALETEXT from crmuser.category_lang a INNER JOIN 
			(select DISTINCT VALUE,CATEGORYID from crmuser.categories v where v.categorytype = 'RB_ANNUAL_INCOME') H
			ON a.categoryid = H.categoryid)J
			ON CORPORATE.STRUSERFIELD21 = J.VALUE
			LEFT JOIN (SELECT z.cif_id,
				   u.value fin_industry_code,
				   u.descr fin_industry_desc
			FROM (
			SELECT ORGKEY CIF_ID, 
				   CASE WHEN a.corp_id IS NOT NULL THEN line_of_activity_desc ELSE industry END industry_type
			FROM   crmuser.accounts a 
				   LEFT JOIN crmuser.corporate c ON a.orgkey=c.corp_key 
			WHERE  industry IS NOT NULL OR
				   line_of_activity_desc IS NOT NULL      
			) z
			INNER JOIN (SELECT VALUE,localetext descr
			FROM   crmuser.categories c
				   INNER JOIN  crmuser.category_lang l ON c.categoryid=l.categoryid
			WHERE  categorytype='INDUSTRY_TYPE' ) u ON z.industry_type=u.value) FIN
			ON ACCOUNTS.ORGKEY = FIN.CIF_ID
			WHERE ACCOUNTS.ENTITY_CRE_FLAG = 'Y'
			AND  ACCOUNTS.ORGKEY IN
			(SELECT DISTINCT CIF_ID FROM (
			SELECT ORGKEY CIF_ID
			FROM   CRMUSER.ACCOUNTS a
			WHERE  TRUNC(bodatecreated)>=TRUNC(SYSDATE-7) OR
				   TRUNC(bodatemodified)>=TRUNC(SYSDATE-7)
			UNION ALL
			SELECT ORGKEY FROM CRMUSER.DEMOGRAPHIC WHERE TRUNC(TMDATE) >= TRUNC(SYSDATE-7)
			UNION ALL
			SELECT CORP_KEY FROM CRMUSER.CORPORATE WHERE TRUNC(TMDATE) >= TRUNC(SYSDATE-7)
			UNION ALL
			SELECT ORGKEY FROM CRMUSER.COREINTERFACE WHERE TRUNC(TMDATE) >= TRUNC(SYSDATE-7)
			UNION ALL
			SELECT ORGKEY FROM CRMUSER.TRADEFINANCE WHERE TRUNC(TMDATE) >= TRUNC(SYSDATE-7)
			UNION ALL
			SELECT ORGKEY FROM CRMUSER.MISCELLANEOUSINFO WHERE TRUNC(TMDATE) >= TRUNC(SYSDATE-7)
			UNION ALL
			SELECT orgkey FROM   crmuser.entitydocument WHERE  trunc(tmdate)>= TRUNC(SYSDATE-7)
			UNION ALL
			SELECT CAST(ENTITY_ID AS NVARCHAR2(20)) ENTITY_ID FROM CFCM WHERE ENTITY_TYPE = 'C' AND 
			TRUNC(LCHG_TIME) >= TRUNC(SYSDATE-7))A)



B.

DWH


DELETE FROM ADF_CDR.CUSTOMER_MASTER
WHERE UPPER(CUSTOMER_SOURCE) = 'FINACLE' AND CUSTOMER_CODE IN (SELECT CUSTOMER_CODE FROM ADF_CLIENT.CUSTOMER_MASTER);


modify ADF_CDR.CUSTOMER_MASTER to combine;

CALL VECTORWISE(COMBINE 'ADF_CDR.CUSTOMER_MASTER+ADF_CLIENT.CUSTOMER_MASTER');

modify ADF_CDR.CUSTOMER_MASTER to combine;



C.

delete from rbl_bi.customer_master_mc 
where customer_code in (select customer_code from adf_client.customer_master);

D.


insert into rbl_bi.customer_master_mc
select  cm.customer_code,cm.pan,cm.aadhaar_number
from adf_client.customer_master cm
where cm.customer_code in (
select a.customer_code
from   adf_client.customer_master a
where  a.customer_code in (select customer_code
             from (
             select z.customer_code,'Y' MC_accounts, case when z1.customer_code is not null then 'Y' else 'N' end Non_MC_accounts
             from
             ( select distinct customer_code
              from   adf_cdr.account_master am
              where  am.product_code in (
              select distinct scheme_code
              from   rbl_bi.dim_product
              where  free_text_10='MC') and 
              closing_date is null) z
              left join ( select distinct customer_code
                          from   adf_cdr.account_master am
                          where  am.product_code not in (
                          select distinct scheme_code
                          from   rbl_bi.dim_product
                          where  free_text_10='MC') and 
                          closing_date is null) z1 on z.customer_code=z1.customer_code) q
                where   Non_MC_accounts='N' ));
				
				
update adf_cdr.customer_master mc
set mc.pan='xxxxxxxxxx',
    mc.aadhaar_number = case when aadhaar_number is not null then 'xxxxxxxxxxxx' else null end
where  mc.customer_code in (
select a.customer_code
from   adf_cdr.customer_master a
where  a.customer_code in (select customer_code
             from (
             select z.customer_code,'Y' MC_accounts, case when z1.customer_code is not null then 'Y' else 'N' end Non_MC_accounts
             from
             ( select distinct customer_code
              from   adf_cdr.account_master am
              where  am.product_code in (
              select distinct scheme_code
              from   rbl_bi.dim_product
              where  free_text_10='MC') and 
              closing_date is null) z
              left join ( select distinct customer_code
                          from   adf_cdr.account_master am
                          where  am.product_code not in (
                          select distinct scheme_code
                          from   rbl_bi.dim_product
                          where  free_text_10='MC') and 
                          closing_date is null) z1 on z.customer_code=z1.customer_code) q
                where   Non_MC_accounts='N' )) ;            				
				
				
				
E.

/*UPDATE ADF_CDR.CUSTOMER_MASTER A
FROM (select distinct cif_no,ucic from adf_cdr.ucic_details) B
SET A.UCIC = B.UCIC
WHERE A.CUSTOMER_CODE = B.CIF_NO;*/

UPDATE ADF_CDR.ucic_details e FROM
(select ROW_NUMBER() OVER (PARTITION BY v.ucic ORDER BY v.ucic) ROW_NUM, v.UCIC,   NVL( O.customer_name,C.CUSTOMER_NAME) CUSTOMER_NAME
from   adf_cdr.ucic_details v left join adf_cdr.customer_master c on v.cif_no=c.customer_code LEFT JOIN
       (select Z.UCIC, y.customer_name 
from   adf_cdr.ucic_details z left join adf_cdr.customer_master y on z.cif_no=y.customer_code
where  nvl(y.create_date,'19000101') in (select max(nvl(a.create_date,'19000101')) cr_dt
from   adf_cdr.ucic_details u left join adf_cdr.customer_master a on u.cif_no=a.customer_code
where  z.ucic=u.ucic) and customer_name is not null) O ON V.UCIC=O.UCIC) D
SET E.UCIC_NAME=D.CUSTOMER_NAME   
WHERE E.UCIC = D.UCIC and row_num = 1;

UPDATE ADF_CDR.CUSTOMER_MASTER A
FROM (select * from(
select row_number()over(partition by cif_no)rno, cif_no,ucic 
from adf_cdr.ucic_details
)z where rno=1) B
SET A.UCIC = B.UCIC
WHERE A.CUSTOMER_CODE = B.CIF_NO;



F.

update adf_cdr.customer_master c
set    c.kyc_flg = 'Y'
where   lower(c.customer_source)='finacle';



update adf_cdr.customer_master c from
(select distinct customer_code
from   adf_cdr.account_master
where  freeze_code is not null and
       freeze_reason_code in ('9','10')) z
set    c.kyc_flg = 'N'
where  c.customer_code=z.customer_code  and
        lower(c.customer_source)='finacle';

		
		
G.

UPDATE ADF_CDR.CUSTOMER_MASTER
SET PHONE_BANKING_FLAG = 'N',DEBIT_FLAG = 'N',INTERNET_BANKING_FLAG = 'N';

update adf_cdr.customer_master cm
set cm.internet_login_date = null,
cm.ib_registration_date = null,
cm.ib_first_txn_date = null,
cm.ib_last_txn_date = null
where cm.internet_banking_flag = 'N';



UPDATE ADF_CDR.CUSTOMER_MASTER CM          ----DEBIT CARD HOTLIST QUERY
FROM (SELECT DISTINCT CUST_ID FROM ADF_CDR.BI_ATM_CARD_MASTER A INNER JOIN ADF_CDR.BI_ATM_HOTLIST_MASTER B
ON A.CARD_NO = B.CARD_NO) DC
SET DEBIT_FLAG = 'H'
WHERE CM.CUSTOMER_CODE = DC.CUST_ID;

UPDATE ADF_CDR.CUSTOMER_MASTER CM          ----DEBIT CARD ACTUAL QUERY
FROM (SELECT CUST_ID,MIN(ISSUE_DATE) ISSUE_DATE,MIN(ACTIVATE_DATE) ACTIVATE_DATE FROM ADF_CDR.BI_ATM_CARD_MASTER A where card_no not in 
(select distinct card_no from adf_cdr.bi_atm_hotlist_master) GROUP BY CUST_ID) DC
SET DEBIT_FLAG = 'Y',
debit_card_issue_date = ISSUE_DATE,
debit_card_activate_date = ACTIVATE_DATE
WHERE CM.CUSTOMER_CODE = DC.CUST_ID;

UPDATE ADF_CDR.CUSTOMER_MASTER
SET DEBIT_FLAG = 'N'
WHERE DEBIT_FLAG NOT IN ('Y','H');

/*UPDATE ADF_CDR.CUSTOMER_MASTER CM
FROM (
SELECT CIF_ID REG_RIB_CIF,min(creation_date) creation_date FROM (
SELECT cif_id,nud_createddate creation_date FROM BSG_READ.ib_cib_reg_user
UNION ALL
SELECT cust_id,created_date FROM BSG_READ.ib_rib_reg_user)a WHERE CIF_ID IS NOT NULL group by cif_iD) IB
SET INTERNET_BANKING_FLAG = 'Y',
IB_REGISTRATION_DATE = CREATION_DATE
WHERE CM.CUSTOMER_CODE = IB.REG_RIB_CIF;


update adf_cdr.customer_master cm 
from (select customer_code,max(log_date_time) log_date_time from (
select user_id customer_code,log_date_time from bsg_read.ib_login 
union all
select CORP_USER customer_code,log_date_time from bsg_read.ib_login WHERE CORP_USER IS NOT NULL)k
GROUP BY CUSTOMER_CODE)K
set cm.internet_login_date = log_date_time
where cm.customer_code = k.customer_code;*/

UPDATE ADF_CDR.CUSTOMER_MASTER CM  ------------IB RIB LOGIN QUERY
FROM (SELECT USER_ID,MAX(CAST(R_CRE_TIME AS DATE)) LAST_LOGIN FROM ADF_CDR.ebank_login 
WHERE EVENT_ID IN ('AUTHENTICATIONFG.VALIDATE_MOBILELOGIN_COMPLETION_WITH_PWD','AUTHENTICATIONFG.VALIDATE_STU_CREDENTIALS_UX') GROUP BY USER_ID)B
SET CM.internet_login_date = B.LAST_LOGIN
WHERE CM.CUSTOMER_CODE = B.USER_ID;

UPDATE ADF_CDR.CUSTOMER_MASTER CM  ------------IB CIB LOGIN QUERY
FROM (Select BRCM.CUST_ID USER_ID,MAX(ADTT.R_CRE_TIME) LAST_LOGIN 
from ADF_CDR.CEBANK_LOGIN ADTT INNER JOIN ADF_CDR.CEBANK_ASDT ASDT ON ADTT.LOG_SRL_NUM = ASDT.LOG_SRL_NUM
LEFT JOIN ADF_CDR.CEBANK_BRCM BRCM ON ADTT.ORG_ID = BRCM.BAY_USER_ID
where ADTT.BANK_ID = '176' AND
ADTT.USER_TYPE = 'CORPORATE' AND ASDT.STATUS <> 'SUCCESS' 
AND ADTT.EVENT_ID = 'AUTHENTICATIONFG.VALIDATE_STU_CREDENTIALS_UX'
GROUP BY BRCM.CUST_ID)B
SET CM.internet_login_date = B.LAST_LOGIN
WHERE CM.CUSTOMER_CODE = B.USER_ID;


update adf_cdr.customer_master cm    -----------IB RIB REGISTRATION QUERY
from (
select cust_id,max(a.login_date) login_date,max(a.last_txn_date) last_txn_date,min(date_format(r_cre_time,'%Y-%m-%d')) registration_date
from adf_Cdr.ebank_master a where a.user_type = '1' and BANK_ID = '176' group by cust_id)b
set 
cm.internet_banking_flag = 'Y',
cm.ib_registration_date = b.registration_date
where cm.customer_code = b.cust_id
and upper(cm.customer_source) = 'FINACLE';

/*
UPDATE ADF_CDR.CUSTOMER_MASTER CM
FROM (
SELECT CIF_ID REG_RIB_CIF,min(creation_date) creation_date FROM (
SELECT cif_id,nud_createddate creation_date FROM BSG_READ.ib_cib_reg_user)a WHERE CIF_ID IS NOT NULL group by cif_iD) IB
SET INTERNET_BANKING_FLAG = 'Y',
IB_REGISTRATION_DATE = CREATION_DATE
WHERE CM.CUSTOMER_CODE = IB.REG_RIB_CIF;*/

UPDATE ADF_CDR.CUSTOMER_MASTER CM   ------IB CIB REGISTRATION QUERY
FROM (SELECT CUST_ID REG_RIB_CIF,MAX(R_CRE_TIME) CREATION_DATE FROM ADF_CDR.cebank_brcm A WHERE A.DEL_FLG = 'N' GROUP BY CUST_ID) IB
SET INTERNET_BANKING_FLAG = 'Y',
IB_REGISTRATION_DATE = CREATION_DATE
WHERE CM.CUSTOMER_CODE = IB.REG_RIB_CIF;

UPDATE ADF_CDR.CUSTOMER_MASTER
SET INTERNET_BANKING_FLAG = 'N'
WHERE INTERNET_BANKING_FLAG <> 'Y';


UPDATE ADF_CDR.CUSTOMER_MASTER CM  ---------MOB REGISTRATION QUERY
FROM (select USERID CUSTOMER_CODE,MIN(createdon) CREATE_DATE from adf_cdr.mob_mb_user_master WHERE NVl(channelid,'x')='MOBL' GROUP BY USERID) MB
SET PHONE_BANKING_FLAG = 'Y',
MOB_REGISTRATION_DATE = MB.CREATE_DATE
WHERE CM.CUSTOMER_CODE = MB.CUSTOMER_CODE;

/*
update adf_cdr.customer_master cm 
from (select customer_code,max(log_date_time) log_date_time from (
select CORP_USER customer_code,log_date_time from bsg_read.ib_login WHERE CORP_USER IS NOT NULL)k
GROUP BY CUSTOMER_CODE)K
set cm.internet_login_date = log_date_time
where cm.customer_code = k.customer_code;*/

update adf_cdr.customer_master cm
set ib_reg_type = null;

/*
UPDATE ADF_CDR.CUSTOMER_MASTER CM 
FROM (
SELECT REG_RIB_CIF,TAG
FROM (
SELECT CIF_ID REG_RIB_CIF,min(creation_date) creation_date,TAG,ROW_NUMBER() OVER (PARTITION BY CIF_ID ORDER BY MIN(CREATION_DATE)) SRNO
FROM (
SELECT cif_id,nud_createddate creation_date,'CIB' TAG FROM BSG_READ.ib_cib_reg_user)a WHERE CIF_ID IS NOT NULL group by cif_iD,TAG)A
WHERE SRNO = '1') B 
SET CM.IB_REG_TYPE = B.TAG
WHERE CM.CUSTOMER_CODE = B.REG_RIB_CIF;*/

UPDATE ADF_CDR.CUSTOMER_MASTER CM     -------IB REG TYPE FOR CIB
FROM (SELECT DISTINCT CUST_ID REG_RIB_CIF,'CIB' TAG FROM ADF_CDR.cebank_brcm A WHERE A.DEL_FLG = 'N' GROUP BY CUST_ID) B 
SET CM.IB_REG_TYPE = B.TAG
WHERE CM.CUSTOMER_CODE = B.REG_RIB_CIF;

update adf_cdr.customer_master a   ----------IB RIB REG TYPE QUERY
from (Select distinct cust_id,'RIB' tag from adf_cdr.ebank_master where BANK_ID = '176') b
set a.IB_REG_TYPE = B.TAG
where a.customer_code = b.cust_id;

/*
update adf_cdr.customer_master cm from (
select User_id customer_code,min(usage_date) ib_first_txn_date,max(usage_date) ib_last_txn_date from (
select IPD_TRANS_DATE usage_date,user_id  FROM "bsg_read". IB_PAYMENT_DTLS WHERE IPD_TRANS_STS<>'F' and user_id is not null 
UNION all
SELECT NTD_VALUE_DATE,(select customer_code from adf_cdr.account_master b where b.account_no=a.ntd_from_acct_no) user_id
FROM "bsg_read". IB_TXN_INFO a WHERE NTD_RESP_STATUS IN('EXE','REJ') and NTD_CUST_REFNO is not null)a group by user_id) ib 
set cm.ib_first_txn_date = ib.ib_first_txn_date,
cm.ib_last_txn_date = ib.ib_last_txn_date
where cm.customer_code = ib.customer_code;*/

update adf_cdr.customer_master cm from   ---- IB FIRST LAST TXN FOR CIB
(SELECT CUST_CODE CUSTOMER_CODE,MIN(TRAN_DATE) ib_first_txn_date,MAX(TRAN_DATE) ib_last_txn_date FROM RBL_BI.fact_transaction_agg1_daily WHERE TYPE = 'IB' AND TXN_TYPE = 'CIB'
GROUP BY CUST_CODE) ib 
set cm.ib_first_txn_date = ib.ib_first_txn_date,
cm.ib_last_txn_date = ib.ib_last_txn_date
where cm.customer_code = ib.customer_code;


update adf_cdr.customer_master a  -------IB FIRST LAST TXN FOR RIB
from (select b.corp_id,min(cast(a.r_cre_time as date)) first_txn_date,max(cast(a.r_cre_time as date)) last_txn_date 
from adf_cdr.ebank_txn_details a inner join adf_cdr.ebank_txn_header b on a.txn_id = b.txn_id group by corp_id) b
set a.ib_last_txn_Date = b.last_txn_date,
a.ib_first_txn_date = b.first_txn_date
where a.customer_code = b.corp_id;

 
UPDATE ADF_CDR.CUSTOMER_MASTER A  -------MOB LOGIN QUERY
FROM (SELECT USERID,MAX(LOGINTIME) LAST_LOGIN_DATE FROM ADF_CDR.MOB_MB_AUDIT WHERE NVL(CHANNELID,'C') = 'MOBL'
GROUP BY USERID)B
SET A.MOB_LOGIN_DATE = B.LAST_LOGIN_DATE
WHERE A.CUSTOMER_CODE = B.USERID;

---- DEBIT CARD USAGE QUERY

UPDATE ADF_cDR.CUSTOMER_MASTER A  
FROM (SELECT B.CUST_ID,MAX(TXN_dATE) LAST_USAGE_DATE
FROM ADF_CDR.BI_ATMPOS_TRANSACTIONS A INNER JOIN ADF_CDR.BI_ATM_CARD_MASTER B ON A.CARD_NO = B.CARD_NO GROUP BY CUST_ID) B 
SET A.DEBIT_CARD_USAGE_DATE = B.LAST_USAGE_DATE
WHERE A.CUSTOMER_CODE = B.CUST_ID;

UPDATE ADF_cDR.CUSTOMER_MASTER A
FROM (SELECT B.CUST_ID,MAX(TXN_dATE) LAST_USAGE_DATE
FROM ADF_CDR.BI_ATMPOS_TRANSACTIONS A INNER JOIN ADF_CDR.BI_ATM_CARD_MASTER B ON A.CARD_NO = B.CARD_NO 
WHERE MERCHANT_TYPE = 'ATM' GROUP BY CUST_ID) B 
SET A.DEBIT_CARD_USAGE_DATE_ATM = B.LAST_USAGE_DATE
WHERE A.CUSTOMER_CODE = B.CUST_ID;

UPDATE ADF_cDR.CUSTOMER_MASTER A
FROM (SELECT B.CUST_ID,MAX(TXN_dATE) LAST_USAGE_DATE
FROM ADF_CDR.BI_ATMPOS_TRANSACTIONS A INNER JOIN ADF_CDR.BI_ATM_CARD_MASTER B ON A.CARD_NO = B.CARD_NO 
WHERE MERCHANT_TYPE = 'POS' GROUP BY CUST_ID) B 
SET A.DEBIT_CARD_USAGE_DATE_POS = B.LAST_USAGE_DATE
WHERE A.CUSTOMER_CODE = B.CUST_ID;

UPDATE ADF_cDR.CUSTOMER_MASTER A
FROM (SELECT B.CUST_ID,MAX(TXN_dATE) LAST_USAGE_DATE
FROM ADF_CDR.BI_ATMPOS_TRANSACTIONS A INNER JOIN ADF_CDR.BI_ATM_CARD_MASTER B ON A.CARD_NO = B.CARD_NO 
WHERE MERCHANT_TYPE = 'ECom' GROUP BY CUST_ID) B 
SET A.DEBIT_CARD_USAGE_DATE_ECom = B.LAST_USAGE_DATE
WHERE A.CUSTOMER_CODE = B.CUST_ID;

UPDATE ADF_CDR.CUSTOMER_MASTER 
SET CARD_TYPE = NULL;

UPDATE ADF_cDR.CUSTOMER_MASTER CM  ---- DEBIT CARD TYPE QUERY
FROM (SELECT CUST_ID,CARD_TYPE FROM (
SELECT CUST_ID,NVL(BT.CARD_TYPE,BF.CARD_TYPE) CARD_TYPE,ROW_NUMBER() OVER (PARTITION BY NVL(BT.CARD_TYPE,BF.CARD_TYPE) ORDER BY CUST_ID) SR_NO
FROM ADF_cDR.bi_atm_card_master BAM 
LEFT JOIN ADF_CDR.BI_CARD_TYPE BT ON LEFT(BAM.CARD_NO,6) = BT.CARD_NO AND BT.identifier = '6'
LEFT JOIN ADF_CDR.BI_CARD_TYPE BF ON LEFT(BAM.CARD_NO,8) = BF.CARD_NO AND BT.identifier = '8'
WHERE BAM.CARD_NO NOT IN (SELECT DISTINCT CARD_NO FROM ADF_cDR.bi_atm_hotlist_master))A WHERE SR_NO = '1') J
SET CM.CARD_TYPE = J.CARD_TYPE
WHERE CM.CUSTOMER_CODE = J.CUST_ID;


delete from adf_cdr.customer_master
where customer_source = 'ECBF' and pan is null
and customer_code in (select customer_Code from adf_cdr.customer_master where customer_source = 'ECBF' 
and customer_code = 'W4706' group by customer_code having count(1)>1);




H.

SELECT PARENTCIFID CHILDCIF,CHILDCIFID PARENTCIF FROM CRMUSER.ENTITYRELATIONSHIP 
WHERE RELATIONSHIP = 'Guardian' AND CHILDCIFID <> 'DEFAULTGUARDIANCIF'


I.

update adf_cdr.customer_master cm
from 
	(select z1.customer_code,z1.create_date,z1.opening_tran_date 
	 from (select z.*, x.dt 
	       from (select  cm.customer_code,cm.create_date, am.opening_tran_date 
			   from   adf_cdr.customer_master cm
                  inner join (select  am.customer_code,min(am.opening_tran_date) opening_tran_date
            			     from adf_cdr.account_master am 
                              inner join bsg_read.rkit_account r on am.account_no=r.account_number
                              group by am.customer_code) am on cm.customer_code = am.customer_code) z
       left  join (select customer_code,min(opening_tran_date) dt from adf_cdr.account_master group by customer_code) x on z.customer_code=x.customer_code and z.opening_tran_date>x.dt) z1
	 where  z1.dt is null)a
set cm.create_date = a.opening_tran_date
where cm.customer_code = a.customer_code;

update adf_cdr.customer_master cm set aadhaar_number=null where left(nvl(cm.aadhaar_number,''),1)!='1' and nvl(cm.aadhaar_number,'')!='';

update adf_cdr.customer_master cm set aadhar_number =null where left(nvl(cm.aadhar_number,''),1)!='1' and nvl(cm.aadhar_number,'')!='';


J.
delete from bsg_read.cust_rm_transfer where TRANSFER_DATE = CURRENT_dATE-1;

insert into bsg_read.cust_rm_transfer
SELECT a.customer_Code,A.rm_name rm_name,B.RM_NAME new_rm,current_date-1 
FROM (SELECT DISTINCT * FROM bsg_read.CUST_RM_TRANSFER_DEPOSITS_RM) A INNER JOIN ADF_cDR.CUSTOMER_MASTER B
ON A.CUSTOMER_cODE = B.CUSTOMER_cODE AND A.rm_name <> B.RM_NAME
WHERE a.balance_date = CURRENT_DATE-2;

K.

update ADF_CDR.CUSTOMER_MASTER A
set A.GROUPID = null,
A.Group_name = null ;

UPDATE ADF_CDR.CUSTOMER_MASTER A
FROM (select DISTINCT CIF_ID,GROUP_ID,GROUP_NAME from adf_cdr.group_id) B
SET A.GROUPID = B.GROUP_ID,
A.GROUP_NAME = B.GROUP_NAME
WHERE A.CUSTOMER_CODE = B.CIF_ID;


modify ADF_CDR.CUSTOMER_MASTER to combine;

CALL VECTORWISE(COMBINE 'ADF_CLIENT.CUSTOMER_MASTER-ADF_CLIENT.CUSTOMER_MASTER');
CALL VECTORWISE(COMBINE 'ADF_CLIENT.CUSTOMER_MASTER');

UPDATE CUSTOMER_MASTER
SET RM_NAME = LPAD(RM_NAME,5,'0')
WHERE LENGTH(NVL(RM_NAME,'NA'))<5 AND UPPER(CUSTOMER_SOURCE) = 'FINACLE';


update adf_cdr.customer_master cm
set    business_segment_desc= (select attrribute_desc
from   adf_cdr.master_codes m
where  cm.business_segment= m.attrribute_code and 
       tag='CRM' and 
       attrribute_type='RB_BUSINESS_SEGMENT')
where cm.business_segment_desc is null
and cm.customer_source='FINACLE' ;



L.


update ADF_CDR.CUSTOMER_MASTER CM
set cm.MODEL_RATING = null,
    cm.MASTER_RATING = null,
    cm.industry_code = null,
    cm.industry_desc = null;
      
update adf_cdr.customer_master cm
from (select * from bsg_read.CFCM a where ENTITY_TYPE = 'C') cfcm
set   cm.MODEL_RATING = cfcm.FREE_CODE_13,
      cm.MASTER_RATING = cfcm.FREE_CODE_14,
      cm.industry_code = CFCM.FREE_CODE_10
where cm.customer_code = cfcm.ENTITY_ID;

update adf_cdr.customer_master cm
from (select * from adf_cdr.master_codes a where a.attrribute_id = 'CC10' and a.tag = 'LCT') b
set   cm.industry_desc = b.attrribute_desc
where cm.industry_code = b.attrribute_code;



M.

update ADF_CDR.customer_master set ASBA_FLG = 'N';

CALL VECTORWISE(COMBINE 'ADF_CDR.CUSTOMER_MASTER');

UPDATE ADF_CDR.customer_master A
FROM (select cif_id from adf_cdr.easba) B
SET A.ASBA_FLG = 'Y'
WHERE A.customer_code = B.cif_id;


N.

drop if exists tmp_cust;
create table tmp_cust as
select *
from (select *,row_number() over(partition by customer_code) rnk from adf_cdr.customer_master  where customer_code in ('W6653','W6835','W3831'))a where rnk = '1';
delete from adf_cdr.customer_master where customer_code in ('W6653','W6835','W3831');
insert into adf_cdr.customer_master(customer_code,create_date,customer_name,gender,address_1,address_2,address_3,city,country,state,zipcode,liability_id,customer_type,defaulter_flag,
risk_rating,suit_flag,psu_flag,iec,rbi_classification,branch_name,branch_code,country_code,pan,id_card_no,customer_source,bsr_org_code,constitution_code,borrower_codes,constitution_desc,
weaker_section,cust_sp_category,female_shg,male_shg,caste,segment,subsegment,business_segment,business_segment_desc,religion_code,religion,phone_home,phone_home2,phone_cell,cust_dob,
email_home,email_palm,email,address_line1,address_line2,address_line3,zip,industry_code,model_rating,master_rating,rm_name,staff_emp_no,dnd_flag,company_code,industry_desc,
internet_banking,sms_banking,debit_card,card_type,salutation,first_name,middle_name,last_name,full_name,occupation,cif_creation_flag,driving_license,userid,voter_id,passport_no,
ration_card_no,specialcategory,minor,preferred_address_type,borrower_description,ucic,groupid,ucic_name,group_name,bc_branch,mobile_availability,village,aadhaar_number,aadhar_seeding,
secondary_rm,kyc_flg,sector_desc,customernreflg,customer_type_code,customer_type_desc,nationality,aadhar_number,education_qualification,employment_status,gross_annual_income,
marital_status,preferredname,maidenname,fin_industry_code,fin_industry_desc,staffflag,internet_banking_flag,sms_banking_flag,phone_banking_flag,card_type_1,card_subtype,debit_flag,
cust_rating_code,cust_rating_date,sourcing_id,internet_login_date,cust_id,mob_login_date,debit_card_usage_date,debit_card_usage_date_atm,debit_card_usage_date_pos,
debit_card_usage_date_ecom,credit_card_flag,ib_registration_date,mob_registration_date,debit_card_issue_date,debit_card_activate_date,ib_first_txn_date,ib_last_txn_date,
kyc_remediation_date,employment_type,ib_reg_type,capacity_of_entity,trade_flg,asba_flg)
select customer_code,create_date,customer_name,gender,address_1,address_2,address_3,city,country,state,zipcode,liability_id,customer_type,defaulter_flag,
risk_rating,suit_flag,psu_flag,iec,rbi_classification,branch_name,branch_code,country_code,pan,id_card_no,customer_source,bsr_org_code,constitution_code,borrower_codes,constitution_desc,
weaker_section,cust_sp_category,female_shg,male_shg,caste,segment,subsegment,business_segment,business_segment_desc,religion_code,religion,phone_home,phone_home2,phone_cell,cust_dob,
email_home,email_palm,email,address_line1,address_line2,address_line3,zip,industry_code,model_rating,master_rating,rm_name,staff_emp_no,dnd_flag,company_code,industry_desc,
internet_banking,sms_banking,debit_card,card_type,salutation,first_name,middle_name,last_name,full_name,occupation,cif_creation_flag,driving_license,userid,voter_id,passport_no,
ration_card_no,specialcategory,minor,preferred_address_type,borrower_description,ucic,groupid,ucic_name,group_name,bc_branch,mobile_availability,village,aadhaar_number,aadhar_seeding,
secondary_rm,kyc_flg,sector_desc,customernreflg,customer_type_code,customer_type_desc,nationality,aadhar_number,education_qualification,employment_status,gross_annual_income,
marital_status,preferredname,maidenname,fin_industry_code,fin_industry_desc,staffflag,internet_banking_flag,sms_banking_flag,phone_banking_flag,card_type_1,card_subtype,debit_flag,
cust_rating_code,cust_rating_date,sourcing_id,internet_login_date,cust_id,mob_login_date,debit_card_usage_date,debit_card_usage_date_atm,debit_card_usage_date_pos,
debit_card_usage_date_ecom,credit_card_flag,ib_registration_date,mob_registration_date,debit_card_issue_date,debit_card_activate_date,ib_first_txn_date,ib_last_txn_date,
kyc_remediation_date,employment_type,ib_reg_type,capacity_of_entity,trade_flg,asba_flg from tmp_cust;
drop table tmp_cust;



3.AAB

FINMIS

A.

select gam.acid,
gsh.gl_code, 
gam.gl_sub_head_code,
gam.foracid Account_No,
case
when gam.acct_ownership = 'O' then
substr(gam.foracid, 1, 5) || substr(gam.foracid, 13, 4)
else
null
end placeholder,
gam.acct_name,
gam.schm_code Scheme_code,
gam.schm_type Scheme_type,
gam.sol_id BRANCH_CODE,
gam.acct_crncy_code ACCOUNT_CCY,
gsp.acct_prefix Account_Prefix,
cast(case
when gam.acct_crncy_code = 'INR' then
eab.tran_date_bal
else
eab.tran_date_bal * rtl.rate
end as float) LCY_Balance,
'${Report_Date}' BALANCE_DATE,
cast(EAB.TRAN_DATE_BAL as float) FCY_BALANCE,
EAB.VALUE_DATE_BAL FCY_BALANCE_VALUEDATE,
cast(nvl(rtl.rate,1) as float) EX_RATE, 
GAC.PD_FLG PAST_DUE_FLAG,
GAC.CHRGE_OFF_FLG,
cast(CASE
WHEN NVL(GAC.CHRGE_OFF_FLG, 'N') = 'Y' OR (gam.acct_cls_Date IS NOT NULL AND ABS(eab.tran_date_bal)=0)  or ABS(eab.tran_date_bal)>0 THEN 0
WHEN NVL(GAC.PD_FLG, 'N') = 'Y' AND GSP.ACCT_PREFIX = '90' THEN ABS(eab.tran_date_bal)
WHEN NVL(GAC.PD_FLG, 'N') = 'N' AND GSP.ACCT_PREFIX = '90' THEN (ABS(eab.tran_date_bal) + NVL(XX.INTEREST_DUE, 0) + NVL(XX.PENAL_DUE, 0) + NVL(XX.OTHER_DUES, 0))
WHEN NVL(GAC.PD_FLG, 'N') = 'Y' AND GSP.ACCT_PREFIX = '80' THEN (ABS(eab.tran_date_bal) - (NVL(XX.INTEREST_DUE, 0) + NVL(XX.PENAL_DUE,0)))
WHEN NVL(GAC.PD_FLG, 'N') = 'Y' AND GSP.ACCT_PREFIX = '60' THEN (ABS(eab.tran_date_bal) - nvl((SELECT SUM(INT_AMT) FROM C_DPD WHERE GAM.ACID=C_DPD.ACID AND INT_AMT>0 AND ENTITY_CRE_FLG='Y' AND DEL_FLG='N'),0))
WHEN SUBSTR(gam.gl_sub_head_code,1,2)='14' THEN ABS(eab.tran_date_bal) ELSE 0
END * (CASE when gam.acct_crncy_code = 'INR' then
1
else
rtl.rate
END) as float) as ADVANCES,
cast(CASE WHEN ABS(eab.tran_date_bal)>=0 THEN 0
WHEN GSP.ACCT_PREFIX = '80' THEN (ABS(eab.tran_date_bal) - (NVL(XX.INTEREST_DUE, 0) + NVL(XX.PENAL_DUE, 0)) + NVL(XX.OTHER_DUES, 0))
WHEN GSP.ACCT_PREFIX = '60' THEN ABS(eab.tran_date_bal) - nvl((SELECT SUM(INT_AMT) FROM C_DPD WHERE GAM.ACID=C_DPD.ACID AND INT_AMT>0 AND ENTITY_CRE_FLG='Y' AND DEL_FLG='N'),0)
WHEN SUBSTR(gam.gl_sub_head_code,1,2)='14' THEN ABS(cast(case
when gam.acct_crncy_code = 'INR' then
eab.tran_date_bal
else
eab.tran_date_bal * rtl.rate
end as float)) ELSE 0
END as float) as PRINCIPAL_OUTSTANDING,
cast (CASE
WHEN GSP.ACCT_PREFIX IN ('80', '90') THEN
NVL(XX.INTEREST_DUE, 0)
WHEN GSP.ACCT_PREFIX IN ('60') THEN
(SELECT SUM(INT_AMT)
FROM C_DPD
WHERE GAM.ACID=C_DPD.ACID AND
INT_AMT>0 AND ENTITY_CRE_FLG='Y' AND DEL_FLG='N') 
ELSE
0
end as float) INTEREST_DUE,
cast ((CASE
WHEN GSP.ACCT_PREFIX IN ('80', '90') THEN
NVL(XX.PENAL_DUE, 0)
ELSE
0
end +
nvl( (SELECT SUM(dmd_amt-tot_adj_amt)
FROM tbaadm.la_pdt u 
WHERE gam.acid=u.acid AND
u.dmd_eff_date<=(SELECT dc_cls_date FROM tbaadm.gct) AND
dmd_flow_id='APDEM' AND
del_flg='N' ),0)) as FLOAT
) PENAL_DUE, 
cast(CASE
WHEN GSP.ACCT_PREFIX IN (80, 90) THEN
NVL(XX.OTHER_DUES, 0)
ELSE
0
end as float) OTHER_DUES,
0 DPD,
cast(gac.dpd_cntr as float) gac_dpd,
(SELECT SUB_CLASSIFICATION_USER
FROM TBAADM.ACD acd
where GAM.ACID = ACD.B2K_ID) ASSET_CLASSIFICATION,
GAM.SANCT_LIM SANCTIONED_LIMIT,SDR.APPORTIONED_VALUE,
cast( CASE
WHEN GSP.ACCT_PREFIX IN ('80', '90') THEN
NVL(XX. PRINCIPLE_DUE, 0)
ELSE
0
end as float) as PRINCIPLE_DUE ,
COALESCE(CAM.ACCT_STATUS,TAM.ACCT_STATUS,LAM.ACCT_STATUS_flg) ACCOUNT_STATUS,
TAM.MATURITY_DATE,
cast(TAM.DEPOSIT_AMOUNT as float) as DEPOSIT_AMOUNT,
CAST((SELECT SUM(chrge_off_principal)
FROM 
(SELECT ACID,chrge_off_principal,chrge_off_date FROM TBAADM.ta_cot WHERE entity_cre_flg='Y' AND del_flg='N' UNION ALL
SELECT ACID,chrge_off_principal,chrge_off_date FROM TBAADM.cot WHERE entity_cre_flg='Y' AND del_flg='N') T
WHERE gam.acid = t.acid AND t.chrge_off_date IS NOT NULL) AS FLOAT) charge_off_principal,
CAST((SELECT SUM(EFFECTIVE_PROV_AMT)
FROM TBAADM.APDT A
WHERE Gam.ACID=A.ACID AND
a.entity_cre_flg='Y' ) AS FLOAT) PROVISION_AMT 
from c_gam GAM
inner join TBAADM.EAB eab ON GAM.ACID = EAB.ACID
INNER join (select distinct gl_code, gl_sub_head_code
from TBAADM.GSH gsh
where gsh.del_flg = 'N') gsh on gam.gl_sub_head_code = gsh.gl_sub_head_code
INNER JOIN TBAADM.GSP gsp on gam.schm_code = gsp.schm_code
LEFT JOIN TBAADM.CAM cam ON GAM.ACID = CAM.ACID 
LEFT JOIN TBAADM.TAM tam ON GAM.ACID = TAM.ACID
left join TBAADM.LAM lam on GAM.ACID = LAM.ACID
LEFT JOIN TBAADM.GAC gac ON GAM.ACID = GAC.ACID
LEFT JOIN (SELECT LDT.ACID,
NVL(SUM(CASE
WHEN LDT.DMD_FLOW_ID = 'INDEM' THEN
NVL((LDT.DMD_AMT - LDT.TOT_ADJ_AMT), 0)
else
0
END),
0) INTEREST_DUE,
NVL(SUM(CASE
WHEN LDT.DMD_FLOW_ID IN ('PIDEM', 'APDEM') THEN
NVL((LDT.DMD_AMT - LDT.TOT_ADJ_AMT), 0)
else
0
END),
0) PENAL_DUE,
NVL(sum(CASE
WHEN LDT.DMD_FLOW_ID NOT IN
('PRDEM', 'PIDEM', 'INDEM', 'APDEM') THEN
nvl((LDT.DMD_AMT - LDT.TOT_ADJ_AMT), 0)
else
0
END),
0) OTHER_DUES,
NVL(SUM(CASE
WHEN LDT.DMD_FLOW_ID = 'PRDEM' THEN
NVL((LDT.DMD_AMT - LDT.TOT_ADJ_AMT), 0)
else
0
END),
0) PRINCIPLE_DUE
FROM TBAADM.LDT ldt
WHERE LDT.DEL_FLG='N'
GROUP BY LDT.ACID) XX
on GAM.acid = XX.acid
left join (select rth.var_crncy_units / rth.fxd_crncy_units rate, rth.*
from TBAADM.rth rth
inner join (select max(lchg_time) max_lchg_time,
max(srl_num) max_srl_num,
rth.FXD_CRNCY_CODE
from TBAADM.rth rth
where rth.var_crncy_code = 'INR'
and rth.ratecode = 'NOR'
and rth.rtlist_date <=
TO_DATE('${Report_Date}', 'DD-Mon-YYYY')
and rth.del_flg = 'N'
group by rth.FXD_CRNCY_CODE) rtl_max on rtl_max.max_srl_num =
rth.srl_num
and rtl_max.max_lchg_time =
rth.lchg_time
and rtl_max.FXD_CRNCY_CODE =
rth.FXD_CRNCY_CODE
where rth.var_crncy_code = 'INR'
and rth.ratecode = 'NOR'
and rth.rtlist_date <= TO_DATE('${Report_Date}', 'DD-Mon-YYYY')
and rth.del_flg = 'N'
) RTL ON GAM.ACCT_CRNCY_CODE = RTL.FXD_CRNCY_CODE
LEFT JOIN (SELECT SUM(SDR.APPORTIONED_VALUE) APPORTIONED_VALUE,SDR.ACID FROM TBAADM.SDR WHERE del_flg='N' AND SDR.USE_FOR_DP_IND IN ('C','P') GROUP BY SDR.ACID) SDR
on GAM.ACID=SDR.ACID
WHERE GAM.DEL_FLG = 'N'
AND GAM.ENTITY_CRE_FLG = 'Y' 
AND TO_DATE('${Report_Date}', 'DD-Mon-YYYY') BETWEEN EAB.EOD_DATE AND EAB.END_EOD_DATE 
AND gsp.del_flg = 'N'






B.

DWH


call vectorwise(combine 'adf_client.all_account_balances');

insert into adf_cdr.all_account_balances(gl_code,gl_sub_head_code,account_no,placeholder,acct_name,scheme_code,scheme_type,branch_code,account_ccy,
account_prefix,lcy_balance,fcy_balance,fcy_balance_valuedate,ex_rate,balance_date,past_due_flag,chrge_off_flg,
advances,principal_outstanding,interest_due,penal_due,other_dues,credit_rate_of_interest,debit_rate_of_interest,
dpd,gac_dpd,asset_classification,sanctioned_limit,apportioned_value,principle_due,account_status,deposit_amount,
td_maturity_date,charge_off_principal,PROVISION_AMT)
select gl_code,gl_sub_head_code,account_no,placeholder,acct_name,scheme_code,scheme_type,branch_code,account_ccy,
account_prefix,lcy_balance,fcy_balance,fcy_balance_valuedate,ex_rate,balance_date,past_due_flag,chrge_off_flg,
advances,principal_outstanding,interest_due,penal_due,other_dues,credit_rate_of_interest,debit_rate_of_interest,
dpd,gac_dpd,asset_classification,sanctioned_limit,apportioned_value,principle_due,account_status,deposit_amount,
td_maturity_date,charge_off_principal,PROVISION_AMT from adf_client.all_account_balances
WHERE BALANCE_DATE = '${Report_Date}';

call vectorwise(combine 'adf_cdr.all_account_balances');

insert into ADF_CDR.at_gl_balance (balance_date,branch_code,gl_id,gl_code,currency,dr_bal,cr_bal,dr_bal_lcy,cr_bal_lcy,batch_date)
SELECT balance_date,branch_code,gl_id,gl_code,currency,dr_bal,cr_bal,dr_bal_lcy,cr_bal_lcy,'${Report_Date}' from adf_cdr.gl_balance
where balance_date='${Report_Date}';

call vectorwise(combine 'ADF_CDR.at_gl_balance');

delete from adf_cdr.gl_balance where balance_date='${Report_Date}';

call vectorwise(combine 'ADF_CDR.gl_balance');

insert into adf_cdr.gl_balance(balance_date,branch_code,gl_id,gl_code,currency,dr_bal,cr_bal,dr_bal_lcy,cr_bal_lcy)
select b.balance_date, branch_code, b.gl_code as gl_id,b.gl_sub_head_code gl_code, b.account_ccy currency,
case when b.fcy_balance<0 then abs(b.fcy_balance) else 0 end dr_bal,
case when b.fcy_balance>=0 then b.fcy_balance else 0 end cr_bal,
case when b.lcy_balance<0 then abs(b.lcy_balance) else 0 end dr_bal_lcy,
case when b.lcy_balance>=0 then b.lcy_balance else 0 end cr_bal_lcy
from
(select branch_code,a.balance_date, a.gl_code,a.gl_sub_head_code, a.account_ccy, sum(a.lcy_balance) lcy_balance, sum(a.fcy_balance) fcy_balance
from adf_client.all_account_balances a where a.balance_date=cast('${Report_Date}' as date)
group by branch_code,a.balance_date, a.gl_code,a.gl_sub_head_code, a.account_ccy ) b;

call vectorwise(combine 'ADF_CDR.gl_balance');

update adf_client.ETL_INTERFACE set LAST_RUN_DATE = '${Report_Date}' where source_id = 'mab_bi';



C.

Excel Output

select c.balance_date,c.gl_code,c.gl_sub_head_code,c.account_no,c.acct_name account_name,c.lcy_balance,c.branch_code as sol_id
from (select distinct gl_code,gl_sub_head_code from adf_cdr.all_account_balances where balance_date=cast('${Report_Date}' as date) and placeholder is not null) a
full join
(select distinct gl_code,gl_sub_head_code from adf_cdr.all_account_balances where balance_date=(select max(balance_date) from adf_cdr.all_account_balances where balance_date<cast('${Report_Date}' as date)) and placeholder is not null) b
on (a.gl_code=b.gl_code and a.gl_sub_head_code=b.gl_sub_head_code)
left join adf_cdr.all_account_balances c on (a.gl_code=c.gl_code and a.gl_sub_head_code=c.gl_sub_head_code)
left join adf_cdr.account_master d on (c.account_no=d.account_no)
left join adf_cdr.customer_master e on (d.customer_code=e.customer_code)
where b.gl_code is null and b.gl_sub_head_code is null
and c.balance_date=cast('${Report_Date}' as date)


D>

FINMIS

SELECT g.FORACID Account_No,
       cast( CASE WHEN tran_date_bal>=0 THEN bo.rbl_getintrate(to_char((SELECT dc_cls_date FROM tbaadm.gct),'dd-mm-yyyy'),g.acid,tran_date_bal,g.schm_type)
            ELSE     0
       END as float)  CREDIT_RATE_OF_INTEREST,
       cast(CASE WHEN tran_date_bal<0 THEN bo.rbl_getintrate(to_char((SELECT dc_cls_date FROM tbaadm.gct),'dd-mm-yyyy'),g.acid,tran_date_bal,g.schm_type)
            ELSE     0
       END as float) DEBIT_RATE_OF_INTEREST,
       cast(CASE WHEN SCHM_TYPE = 'TDA' AND t.deposit_Status='P'  THEN
       (SELECT full_rate
        FROM   tbaadm.IDT I
        WHERE  G.ACID=I.ENTITY_ID AND
               I.interest_type='N'  AND
               I.SERIAL_NUM IN (SELECT MAX(SERIAL_NUM) FROM tbaadm.IDT J WHERE G.ACID=J.ENTITY_ID AND J.interest_type='N') and rownum<2)
            WHEN  SCHM_TYPE = 'TDA' AND t.deposit_Status='O'  THEN
               (SELECT nvl(full_rate,0) + nvl(DIFF_RATE,0)
            FROM   tbaadm.IDT I 
            WHERE  G.ACID=I.ENTITY_ID AND
                         I.ENTITY_TYPE='ACCNT' AND
                         I.interest_type='O' AND
                           I.ENTITY_CRE_FLG='Y' AND
                           (SELECT dc_cls_date FROM tbaadm.gct) BETWEEN START_DATE AND END_DATE) 
             else 0
        end as float) OD_RATE  ,
        (SELECT dc_cls_date FROM tbaadm.gct) rate_date,
       0 CUST_CR_PREF_PCNT,
       0 ID_CR_PREF_PCNT,
       0 NRML_PCNT_CR,
       0 CUST_DR_PREF_PCNT,
       0 ID_DR_PREF_PCNT,
       0 NRML_PCNT_DR
FROM   C_GAM G,
       TBAADM.EAB E,
       TBAADM.TAM T
WHERE  (SELECT dc_cls_date FROM tbaadm.gct) BETWEEN EOD_DATE AND END_EOD_DATE           AND
       e.ACID = g.ACID                                              AND
       CIF_ID IS NOT NULL                                           AND
       g.del_flg='N'                                                AND
       g.entity_cre_flg='Y'                                         AND
       G.ACID = T.ACID(+)

	   
	   
E.

update all_account_balances a set credit_rate_of_interest=(select credit_rate_of_interest from all_acctbal_intrate b where b.rate_date=a.balance_date and a.account_no=b.account_no)
where balance_date='${Report_Date}';


update all_account_balances a set debit_rate_of_interest=(select debit_rate_of_interest from all_acctbal_intrate b where b.rate_date=a.balance_date and a.account_no=b.account_no)
where balance_date='${Report_Date}';



4. AAB2

a  EXTRACT FROM FINMISstore AND STORE IN :- ROI_STEP_N

	SELECT g.ACID,FORACID ACCOUNT_NO,CASE WHEN INTEREST_IND ='D' THEN ABS(FULL_RATE) +ABS(DIFF_RATE) ELSE 0 END debit_int_rate,
        CASE WHEN INTEREST_IND ='C' THEN ABS(FULL_RATE) +ABS(DIFF_RATE) ELSE 0 END credit_int_rate,
        SCHM_TYPE,gl_sub_head_code
            FROM   TBAADM.IDT T, (SELECT acid,gl_sub_head_code,foracid,schm_type
                FROM   tbaadm.gam
                WHERE  acct_cls_date IS NULL AND
                       acct_ownership<>'O') g, 
                   (SELECT acid
                  FROM   tbaadm.gac
                  WHERE  nvl(MARKUP_INT_RATE_APPL_FLG,'N')='N'   ) c
            WHERE  (select dc_cls_date from tbaadm.gct)  between start_date and end_date AND  
                   INTEREST_TYPE = 'N'  AND entity_type='ACCNT' AND            
                   interest_ind = case when substr(gl_sub_head_code,1,2)='14' then 'D' else 'C' end and                       
                   T.ENTITY_CRE_FLG = 'Y' AND
                   T.ENTITY_ID = G.ACID  AND  ABS(FULL_RATE) +ABS(DIFF_RATE)>0 AND
                   g.schm_type IN ('ODA','SBA','LAA','CLA','PCA','FBA') AND 
                   g.acid=c.acid;
				   
	SELECT g.ACID,g.FORACID ACCOUNT_NO, int_rate debit_int_rate,0 credit_int_rate,
		  SCHM_TYPE,gl_sub_head_code
		FROM   TBAADM.IDdT T
		INNER JOIN  tbaadm.gac c ON t.entity_id=c.acid AND nvl(C.MARKUP_INT_RATE_APPL_FLG,'N')='Y' --AND t.entity_id='011471031'
		INNER JOIN TBAADM.GAM g ON T.ENTITY_ID = G.ACID  AND  g.schm_type IN ('ODA','LAA','CLA') AND g.acct_cls_date IS NULL
		INNER JOIN (SELECT entity_id,SERIAL_NUM,detail_num,row_number() over(PARTITION BY entity_id ORDER BY  entity_id,SERIAL_NUM DESC,detail_num DESC) srno
		FROM tbaadm.iddt WHERE interest_level='C' AND INTEREST_IND='D'   AND INTEREST_TYPE='N') z ON t.entity_id=z.entity_id AND t.serial_num=z.serial_num AND 
		t.detail_num=z.detail_num AND z.srno=1 AND 
	    interest_level='C' AND INTEREST_IND='D'   AND INTEREST_TYPE='N'	;
		
	EXTRACT FROM FINMIS AND STORE IN :- ROI_STEP_1
	SELECT g.acid,FORACID ACCOUNT_NO,CUST_CR_PREF_PCNT+ID_CR_PREF_PCNT+NRML_PCNT_CR LOC_INT_RATE,SCHM_TYPE,gl_sub_head_code
        FROM   TBAADM.ITC I, TBAADM.gam g,TBAADM.TAM
        WHERE  g.schm_type='TDA'       and
               (g.acct_cls_date IS NULL or g.acct_cls_date >= TO_DATE('${Report_Date}','DD-MM-YYYY'))  AND
               G.ACID = TAM.ACID AND 
               TAM.MATURITY_DATE >= TO_DATE('${Report_Date}','DD-MM-YYYY') AND
               g.acid= I.ENTITY_ID  AND
               I.ENTITY_TYPE ='ACCNT' AND NRML_PCNT_CR > 0 AND
               I.INT_TBL_CODE_SRL_NUM IN (SELECT MAX(INT_TBL_CODE_SRL_NUM)
                                          FROM   TBAADM.ITC Y
                                          WHERE  I.ENTITY_ID=Y.ENTITY_ID AND
                                                 I.ENTITY_TYPE = Y.ENTITY_TYPE AND
                                                 TO_DATE(TO_CHAR(Y.RECORD_DATE,'DD-MM-YYYY'),'DD-MM-YYYY') <= TO_DATE('${Report_Date}','DD-MM-YYYY') AND
                                                 Y.DEL_FLG='N' AND
                                                 Y.ENTITY_CRE_FLG='Y');	
												 
	SELECT z.ACID,ACCOUNT_NO,loc_int_rate,SCHM_TYPE,gl_sub_head_code
	FROM   
	(SELECT row_number() over(PARTITION BY entity_id ORDER BY serial_num DESC) srno, G.ACID,
	FORACID ACCOUNT_NO, ABS(FULL_RATE) +ABS(DIFF_RATE)  loc_int_rate,SCHM_TYPE,gl_sub_head_code
				FROM   TBAADM.IDT T, TBAADM.gam g,tbaadm.tam
				WHERE g.schm_type IN ('TDA') AND 
					 g.acct_cls_date IS NULL  AND
					 G.ACID = TAM.ACID AND
					 TAM.MATURITY_DATE < TO_DATE('${Report_Date}','DD-MM-YYYY') AND
					  G.ACID = T.ENTITY_ID  AND  
					  INTEREST_IND =  'C'   AND
					  INTEREST_TYPE = 'O'  AND
					  T.ENTITY_CRE_FLG = 'Y' AND
					  TO_DATE('${Report_Date}','DD-MM-YYYY')  between start_date and end_date) Z
					   where   Z.srno=1;
					   
	SELECT z.*, CASE WHEN z.LOC_INT_RATE=0 THEN nvl(x.full_rate,0)+nvl(x.diff_rate,0) ELSE z.LOC_INT_RATE END LOC_INT_RATE1
	FROM (                                                  
	SELECT g.acid,FORACID ACCOUNT_NO,CASE WHEN NRML_PCNT_CR=0 THEN 0 ELSE  CUST_CR_PREF_PCNT+ID_CR_PREF_PCNT+NRML_PCNT_CR END LOC_INT_RATE,SCHM_TYPE,gl_sub_head_code
			FROM   TBAADM.ITC I
				   INNER JOIN TBAADM.gam g ON  I.ENTITY_ID=g.acid AND I.ENTITY_TYPE ='ACCNT' AND g.schm_type='TDA' AND g.acct_cls_date IS NULL 
					inner join TBAADm.TAM on  G.ACID = TAM.ACID AND TAM.MATURITY_DATE >= TO_DATE('${Report_Date}','DD-MM-YYYY') 
		   WHERE i.lchg_time IN (SELECT MAX(y.lchg_time)
											  FROM   TBAADM.ITC Y
											  WHERE  I.ENTITY_ID=Y.ENTITY_ID AND
													 I.ENTITY_TYPE = Y.ENTITY_TYPE AND
													 Y.DEL_FLG='N' AND
													 Y.ENTITY_CRE_FLG='Y')  AND 
				  nvl(NRML_PCNT_CR,0)=0) z                                               
	LEFT JOIN tbaadm.idt x ON z.acid=x.entity_id AND x.interest_type='N' AND x.interest_ind='C' AND
                                       TO_DATE('${Report_Date}','DD-MM-YYYY') BETWEEN x.start_Date AND x.end_date AND z.LOC_INT_RATE=0  ;
									   
									   
	--THIS IS TO GET LIST OF ACCOUNTS WHERE THE ROI IS ZER0. THIS DATA IS STORED IN FINRECON
	SELECT DISTINCT ACCOUNT_NO FROM (
	SELECT  a.account_no FROM adf_client.ROI_STEP_n A INNER JOIN ADF_CDR.ACCOUNT_MASTER B 
	ON A.ACCOUNT_NO = B.ACCOUNT_NO WHERE (nvl(credit_roi,0) = 0 and nvl(debit_roi,0) = 0) or (nvl(credit_roi,0) = 0 and left(b.gl_sub_head_code,2)='23') or
	(nvl(debit_roi,0) = 0 and  left(b.gl_sub_head_code,2)='14')
	UNION ALL
	SELECT  a.account_no FROM adf_client.ROI_STEP_1 A INNER JOIN ADF_CDR.ACCOUNT_MASTER B 
	ON A.ACCOUNT_NO = B.ACCOUNT_NO WHERE LOC_INT_RATE = 0
	UNION ALL
	SELECT ACCOUNT_NO
	FROM   ADF_CDR.ACCOUNT_MASTER
	WHERE  CLOSING_DATE IS NULL AND
		   ACCOUNT_NO NOT IN (SELECT ACCOUNT_NO FROM adf_client.ROI_STEP_N) AND
			ACCOUNT_NO NOT IN (SELECT ACCOUNT_NO FROM adf_client.ROI_STEP_1) AND
		   NVL(ACCOUNT_STATUS,'A') NOT IN ('D','I')) Z;
		   
	-- COPY ALL THE RECORDS OF DAY BEFORE YESTERDAY TO YESTERDAY :-
    DELETE FROM ADF_CDR.ALL_ACCTBAL_INTRATE WHERE RATE_DATE = CURRENT_DATE-1;
	INSERT INTO ADF_CDR.ALL_ACCTBAL_INTRATE
	SELECT account_no,debit_rate_of_interest,credit_rate_of_interest,od_rate,CURRENT_DATE-1 rate_date,cust_cr_pref_pcnt,
	id_cr_pref_pcnt,nrml_pcnt_cr,cust_dr_pref_pcnt,id_dr_pref_pcnt,nrml_pcnt_dr
	FROM ADF_CDR.ALL_ACCTBAL_INTRATE
	WHERE RATE_DATE = CURRENT_DATE-2;	

	--FUNCTIONS TO GET THE ROI FROM MAPPER TABLES IN FINMIS
	SELECT foracid ACCOUNT_NO,
		   CASE WHEN substr(g.gl_sub_head_code,1,2)='23' OR p.acct_prefix IN ('30') THEN 
					 bsg.RBL_GetZeroBalIntRate_N(c.int_tbl_code,c.pegged_flg,g.acct_crncy_code,c.int_version,p.acct_prefix,'C' ,g.schm_type,cust_cr_pref_pcnt,id_cr_pref_pcnt,0,0,c.start_date,(select to_char(trunc(sysdate)-1,'dd-mon-yyyy') from dual))
				ELSE 0
		   END  CREDIT_RATE_OF_INTEREST,
		   CASE WHEN substr(g.gl_sub_head_code,1,2)='14' OR p.acct_prefix IN ('60','40','80','90') THEN 
					 bsg.RBL_GetZeroBalIntRate_N(c.int_tbl_code,c.pegged_flg,g.acct_crncy_code,c.int_version,p.acct_prefix,'D' ,g.schm_type,0,0,cust_dr_pref_pcnt,id_dr_pref_pcnt,c.start_date,(select to_char(trunc(sysdate)-1,'dd-mon-yyyy') from dual))
				ELSE 0
		   END  debit_rate_of_interest,
		   0 OD_RATE,
		trunc(SYSDATE)-1 rate_date                       
	FROM   bsg.temp_intrate_account_no t
	INNER JOIN tbaadm.gam@link_mis g ON t.account_no=g.foracid
	INNER JOIN tbaadm.gsp@link_mis p ON g.schm_code=p.schm_code --AND p.acct_prefix='40' 
	INNER JOIN tbaadm.itc@link_mis c ON g.acid=c.entity_id 
	WHERE  c.INT_TBL_CODE_SRL_NUM IN (SELECT MAX(INT_TBL_CODE_SRL_NUM)
						   FROM   tbaadm.itc@link_mis y
						   WHERE  c.entity_id=y.entity_id AND
								  y.entity_cre_flg='Y' AND
								  y.del_flg='N' and y.start_Date <= trunc(sysdate)-1);
								  
	-- UPDATE 
	delete from ADF_CDR.all_acctbal_intrate where rate_date = '${Report_Date}'
	AND ACCOUNT_NO IN (SELECT DISTINCT ACCOUNT_NO FROM ADF_CLIENT.ROI_STEP_N);

	INSERT INTO ADF_CDR.ALL_ACCTBAL_INTRATE
	SELECT account_no,debit_roi debit_rate_of_interest,
	credit_roi Credit_rate_of_interest,0,
	current_date-1,0,0,0,0,0,0
	FROM ADF_CLIENT.ROI_STEP_N;


	delete from ADF_CDR.all_acctbal_intrate where rate_date = '${Report_Date}'
	AND ACCOUNT_NO IN (SELECT DISTINCT ACCOUNT_NO FROM ADF_CLIENT.ROI_STEP_1);


	INSERT INTO ADF_CDR.ALL_ACCTBAL_INTRATE
	SELECT account_no,case when left(gl_sub_head_code,2) = '14' then loc_int_rate else 0 end debit_rate_of_interest,
	case when left(gl_sub_head_code,2) = '23' then loc_int_rate else 0 end Credit_rate_of_interest,0,
	current_date-1,0,0,0,0,0,0
	FROM ADF_CLIENT.ROI_STEP_1;


	--delete from ADF_CDR.all_acctbal_intrate where rate_date = '${Report_Date}'
	--AND ACCOUNT_NO IN (SELECT DISTINCT ACCOUNT_NO FROM ADF_CLIENT.TEMP_ALL_ACCTBAL_INTRATE);

	INSERT INTO ADF_CDR.ALL_ACCTBAL_INTRATE
	SELECT * FROM ADF_CLIENT.TEMP_ALL_ACCTBAL_INTRATE x
	where  x.account_no not in (select account_no from ADF_CDR.ALL_ACCTBAL_INTRATE) ;

	update ADF_CDR.ALL_ACCTBAL_INTRATE x from
	(SELECT * FROM ADF_CLIENT.TEMP_ALL_ACCTBAL_INTRATE v where v.debit_rate_of_interest>0 ) y
	set x.debit_rate_of_interest=y.debit_rate_of_interest
	where  x.account_no=y.account_no and x.rate_date = '${Report_Date}' and
	x.debit_rate_of_interest=0;


	update ADF_CDR.ALL_ACCTBAL_INTRATE x from
	(SELECT * FROM ADF_CLIENT.TEMP_ALL_ACCTBAL_INTRATE v where v.credit_rate_of_interest>0 ) y
	set x.credit_rate_of_interest=y.credit_rate_of_interest
	where  x.account_no=y.account_no and x.rate_date = '${Report_Date}' and
	x.credit_rate_of_interest=0;
	
    update adf_cdr.all_account_balances a FROM 
	(select * from adf_cdr.all_acctbal_intrate b where b.rate_date=CURRENT_DATE-1) B
	set A.credit_rate_of_interest = B.CREDIT_RATE_OF_INTEREST
	where A.ACCOUNT_NO = B.ACCOUNT_NO AND A.balance_date=CURRENT_DATE-1 AND A.LCY_BALANCE>=0	
	
	update adf_cdr.all_account_balances a FROM 
	(select * from adf_cdr.all_acctbal_intrate b where b.rate_date=CURRENT_DATE-1) B
	set A.debit_rate_of_interest = B.debit_rate_of_interest
	where A.ACCOUNT_NO = B.ACCOUNT_NO AND A.balance_date=CURRENT_DATE-1 AND A.LCY_BALANCE<=0;
	
	update adf_cdr.account_master x from 
	(select V.account_no,case when v.lcy_balance<0 then V.debit_rate_of_interest
	else V.credit_rate_of_interest 
	end roi
	from adf_cdr.all_account_balances v where BALANCE_DATE = CURRENT_DATE-1 AND placeholder IS NULL) y 
	set x.interest_rate=y.roi where  x.account_no=y.account_no;
	
	update adf_cdr.all_acctbal_intrate i
	set debit_rate_of_interest = nvl((select nvl(x.avg_roi,0)
	from (
		select  
		distinct 
		account_no,tag1, sum_of_prod_A_and_B /  cast(sum_of_A as decimal(25,6)) Avg_Roi
			from (
					select tag1,b.account_no, nvl(int_rate,0) int_rate, inr_nor_value,inr_nor_value * nvl(int_rate,0) C,
					sum(inr_nor_value * nvl(int_rate,0)) over(partition by b.account_no ) sum_of_prod_A_and_B,
						   sum(inr_nor_value) over(partition by b.account_no ) sum_of_A, am.account_prefix
					from   adf_cdr.pca_bills_balances b
						   inner join adf_cdr.account_master am on b.account_no=am.account_no and am.scheme_type in ('PCA','FBA')
					where  balance_date='${Report_Date}'
				) z 
		) x
	where  i.account_no=x.account_no),i.debit_rate_of_interest)
	where i.rate_date='${Report_Date}' and
		  i.account_no in (select account_no from adf_cdr.account_master v where v.scheme_type in  ('PCA','FBA'));
		  
-->>	INTEREST DETAILS	  

    EXTRACT FROM FINRECON :-
	
	SELECT gam.FORACID  as ACCOUNT_NO,
		   TO_CHAR((SELECT DC_CLS_DATE FROM TBAADM.GCT@link_mis),'DD-MON-YYYY') BALANCE_DATE,
		   CAST(E.NRML_BOOKED_AMOUNT_CR - NRML_INTEREST_AMOUNT_CR AS FLOAT) Int_payable,
		   CAST(E.NRML_BOOKED_AMOUNT_DR - NRML_INTEREST_AMOUNT_DR AS FLOAT) Int_Receivable,
		   CAST(E.PENAL_BOOKED_AMOUNT_DR - E.PENAL_INTEREST_AMOUNT_DR AS FLOAT) PENAL_Int_Receivable,
		   CAST(E.OVDU_BOOKED_AMOUNT_DR-OVDU_INTEREST_AMOUNT_DR AS FLOAT) OVDU_INTEREST_payable,
		   E.LAST_INTEREST_RUN_DATE_CR,
		   E.LAST_INTEREST_RUN_DATE_DR,
		   E.CRNCY_CODE,
		   E.NRML_BOOKED_AMOUNT_CR,
		   NRML_INTEREST_AMOUNT_CR,
		   E.NRML_BOOKED_AMOUNT_DR,
		   NRML_INTEREST_AMOUNT_DR,
		   E.PENAL_BOOKED_AMOUNT_DR,
		   E.PENAL_INTEREST_AMOUNT_DR,
		   E.OVDU_BOOKED_AMOUNT_DR,
		   OVDU_INTEREST_AMOUNT_DR ,
		   E.NEXT_PEG_REVIEW_DATE  ,
		   GAM.DRWNG_POWER,
		   v.chrge_off_amt,
		   m.int_tbl_code,
		   m.normal_sdate, 
		   m.mclr_sdate,
		   l.repricing_plan,
		   itc.PEGGED_FLG,
		   itc.peg_frequency_in_months,
		   itc.peg_frequency_in_days,
		   e.Next_int_run_date_dr,
		   e.Next_int_run_date_cr,
		   gam.chrg_level_code,
		   CASE WHEN GAM.LIMIT_B2KID IS NOT NULL THEN LLT.LIM_exp_DATE ELSE LHT.lim_exp_date END LIMIT_expiry_DT,
		   e.int_freq_type_dr
	FROM   C_GAM@link_mis GAM 
	LEFT JOIN ${BACKUP_DATE} E ON GAM.ACID = E.ENTITY_ID AND E.ENTITY_TYPE='ACCNT'
	left join 
			  (SELECT ACID,
					  MAX(chrge_off_date) chrge_off_date,
					  sum(chrge_off_principal) chrge_off_amt
			   FROM (SELECT ACID, chrge_off_principal, chrge_off_date
					 FROM tbaadm.ta_cot@link_mis WHERE entity_cre_flg = 'Y' AND del_flg = 'N'
					 UNION ALL
					 SELECT ACID, chrge_off_principal, chrge_off_date
					 FROM tbaadm.cot@link_mis WHERE entity_cre_flg = 'Y' AND del_flg = 'N') T
					 WHERE t.chrge_off_date IS NOT NULL
					 GROUP BY ACID)v on gam.acid = V.acid
	 left join (SELECT acid,foracid,i.int_tbl_code,normal_sdate, mclr_sdate
			   FROM   tbaadm.gam@link_mis g
			   INNER JOIN tbaadm.itc@link_mis i ON g.acid=i.entity_id
			   INNER JOIN (SELECT entity_id,MAX(int_tbl_code_srl_num) srl_num ,MIN(start_date) normal_sdate
						  FROM   tbaadm.itc@link_mis 
						  WHERE  entity_cre_flg='Y' AND del_flg='N' 
						  GROUP BY entity_id) u ON i.entity_id=u.entity_id AND i.int_tbl_code_srl_num=u.srl_num
						  LEFT JOIN (SELECT entity_id,MIN(start_date) mclr_sdate
						  FROM   tbaadm.itc@link_mis 
						  WHERE  entity_cre_flg='Y' AND del_flg='N'  AND substr(int_tbl_code,2,4) IN ('BMCL','CMCL','MCLR','YBMC','YCMC','YMCL')
						  GROUP BY entity_id) v ON i.entity_id=v.entity_id           
						  WHERE acct_cls_date IS NULL )  m on  m.acid = gam.acid
	left join tbaadm.lrp@link_mis l on gam.acid = l.acid   
	LEFT JOIN (SELECT A.ENTITY_ID, PEGGED_FLG,peg_frequency_in_months,peg_frequency_in_days
		  FROM TBAADM.ITC@link_mis  A,
		  (SELECT ENTITY_ID,
		  MAX(INT_TBL_CODE_SRL_NUM) INT_TBL_CODE_SRL_NUM
		  FROM  TBAADM.ITC@link_mis
		  WHERE ENTITY_TYPE='ACCNT'
		  AND   DEL_FLG = 'N'
		  AND   ENTITY_CRE_FLG = 'Y'
		  GROUP BY ENTITY_ID) B
		  WHERE A.ENTITY_ID = B.ENTITY_ID
		  AND A.INT_TBL_CODE_SRL_NUM = B.INT_TBL_CODE_SRL_NUM) ITC ON  GAM.ACID = ITC.ENTITY_ID   
	LEFT JOIN (SELECT LIMIT_B2KID,LIM_EXP_DATE,LIMIT_REVIEW_DATE FROM TBAADM.LLT@link_mis) LLT ON GAM.LIMIT_B2KID=LLT.LIMIT_B2KID 
	LEFT JOIN (SELECT A.ACID, SUM(A.SANCT_LIM) SANCT_LIM, MIN(A.LIM_SANCT_DATE) LIM_SANCT_DATE,MIN(A.LIM_REVIEW_DATE) LIM_REVIEW_DATE,
				MIN(a.lim_exp_date) lim_exp_date ,MIN(A.APPLICABLE_DATE) APPLICABLE_DATE
			  FROM TBAADM.LHT@link_mis A WHERE  a.status='A'  AND ENTITY_CRE_FLG='Y' AND DEL_FLG='N'
			  GROUP BY ACID) LHT ON  GAM.ACID=LHT.ACID                                     
	WHERE  GAM.CIF_ID IS NOT NULL AND
		   (GAM.ACCT_CLS_DATE IS NULL OR GAM.ACCT_CLS_DATE >= (SELECT DC_CLS_DATE FROM tbaadm.GCT@link_mis));
	   
	   --UPDATES

        UPDATE adf_CLIENT.INTEREST_DETAILS A
		FROM (select distinct x.account_ccy ,ex_rate from adf_cdr.all_account_balances  x 
		where balance_date=current_date-1) B
		SET A.ex_rate = B.ex_rate
		WHERE A.crncy_code = B.account_ccy;	   
		

-->>	HTD

		EXTRACT FROM FINMIS :-
		
		SELECT  H.*,
				 NULL COMPANY_CODE,
								1 exchange_rate,  
				 CASE WHEN (H.ENTRY_USER_ID='CDCI' AND x.DELIVERY_CHANNEL_ID='ATM' AND H.TRAN_TYPE='T' AND SUBSTR(H.TRAN_PARTICULAR,1,3)<>'AFT') THEN 0 --'ATM' 
					  WHEN H.RCRE_USER_ID IN ('FIVUSR','NEFT') AND H.TRAN_TYPE='T' AND H.TRAN_SUB_TYPE='CI' THEN 0 -- 'IB'
					  WHEN H.TRAN_TYPE='L'THEN 0 --'CLEARING'
					  WHEN TO_NUMBER(DTH_INIT_SOL_ID)<900 THEN 1 --'BRANCH TRN'
				 END BRANCH_TRANSACTIONS,
			   CASE WHEN (H.ENTRY_USER_ID='CDCI' AND x.DELIVERY_CHANNEL_ID='ATM' AND H.TRAN_TYPE='T'  AND SUBSTR(H.TRAN_PARTICULAR,1,3)<>'AFT') THEN 0 --'ATM' 
					  WHEN H.RCRE_USER_ID IN ('FIVUSR','NEFT') AND H.TRAN_TYPE='T' AND H.TRAN_SUB_TYPE='CI' THEN 0 -- 'IB'
					  WHEN H.TRAN_TYPE='L'THEN 0 --'CLEARING'
					  WHEN TO_NUMBER(DTH_INIT_SOL_ID)>=900 THEN 1 --'non BRANCH TRN'
				 END NON_BRANCH_TRANSACTIONS,
				 CASE WHEN (H.ENTRY_USER_ID='CDCI' AND x.DELIVERY_CHANNEL_ID='ATM' AND H.TRAN_TYPE='T' AND SUBSTR(H.TRAN_PARTICULAR,1,3)<>'AFT') THEN 1 --'ATM' 
					  WHEN H.RCRE_USER_ID IN ('FIVUSR','NEFT') AND H.TRAN_TYPE='T' AND H.TRAN_SUB_TYPE='CI' THEN 0 -- 'IB'
					  WHEN H.TRAN_TYPE='L'THEN 0 --'CLEARING'
					  ELSE 0 --'BRANCH TRN'
				 END ATM_TRANSACTIONS,
				 CASE WHEN (H.ENTRY_USER_ID='CDCI' AND x.DELIVERY_CHANNEL_ID='ATM' AND H.TRAN_TYPE='T' AND SUBSTR(H.TRAN_PARTICULAR,1,3)<>'AFT') THEN 0 --'ATM' 
					  WHEN H.RCRE_USER_ID IN ('FIVUSR','NEFT') AND H.TRAN_TYPE='T' AND H.TRAN_SUB_TYPE='CI' THEN 1 -- 'IB'
					  WHEN H.TRAN_TYPE='L'THEN 0 --'CLEARING'
					  ELSE 0 --'BRANCH TRN'
				 END IB_TRANSACTIONS,
				 CASE WHEN (H.ENTRY_USER_ID='CDCI' AND x.DELIVERY_CHANNEL_ID='ATM' AND H.TRAN_TYPE='T' AND SUBSTR(H.TRAN_PARTICULAR,1,3)<>'AFT') THEN 0 --'ATM' 
					  WHEN H.RCRE_USER_ID IN ('FIVUSR','NEFT') AND H.TRAN_TYPE='T' AND H.TRAN_SUB_TYPE='CI' THEN 0 -- 'IB'
					  WHEN H.TRAN_TYPE='L'THEN 1 --'CLEARING'
					 ELSE 0 --'BRANCH TRN'
				 END CLEARING_TRANSACTIONS,
				 CASE WHEN  (H.TRAN_TYPE='T' AND H.TRAN_SUB_TYPE IN ('CI','BI') AND (NVL(REF_NUM,' ')<>' ' OR MODULE_ID IN ('SWI','PIS'))  AND (INSTR(H.TRAN_PARTICULAR,'NEFT')>0  OR INSTR(H.TRAN_PARTICULAR,'RTGS')>0)) THEN 'RTGS_NEFT'
					  WHEN ((H.TRAN_TYPE='C' AND H.TRAN_SUB_TYPE IN ('CR','CP','RI','NP','PI','NR')) OR
							(H.TRAN_TYPE || H.TRAN_SUB_TYPE || H.PART_TRAN_TYPE IN ('LID','LOC') AND INSTR(TRAN_PARTICULAR,'CHQ DEPOSIT RETURN')=0)    OR
							(H.TRAN_TYPE='T' AND H.TRAN_SUB_TYPE IN ('SI','O','I','EI'))             OR
							(H.TRAN_TYPE='T' AND H.TRAN_SUB_TYPE = 'CI' AND INSTR(TRAN_PARTICULAR,'ATR:')=0)  OR
							(H.TRAN_TYPE='T' AND H.TRAN_SUB_TYPE = 'BI' AND (H.MODULE_ID IN ('REM','FEX') OR SUBSTR(H.TRAN_PARTICULAR,1,4)='NACH'))) THEN 'CUST_INDUCED'
					  WHEN  H.TRAN_TYPE='T' AND H.TRAN_SUB_TYPE='CI' AND H.ENTRY_USER_ID IN ('CDCI','FIVUSR','NEFT') THEN  'CUST_INDUCED'
					  WHEN  H.TRAN_TYPE='T' AND x.DELIVERY_CHANNEL_ID='ASB' THEN 'CUST_INDUCED'
					  WHEN  H.TRAN_TYPE='T' AND H.TRAN_SUB_TYPE='BI' AND H.ENTRY_USER_ID = 'FIVUSR' THEN  'CUST_INDUCED'
					  
					 ELSE  'BANK_INDUCED'
				 END  CUST_BANK_INDUCED ,
				 (select G.foracid from tbaadm.gam g where g.acid=h.acid) as account_no,
				 CASE WHEN  (H.TRAN_TYPE = 'C' OR (H.TRAN_TYPE = 'T' AND H.RPT_CODE IN ('DSBD','DSBP','BEAT','CMSCA','ACASH','LRCAS'))  OR (H.ENTRY_USER_ID='CDCI' AND x.DELIVERY_CHANNEL_ID='ATM' AND SUBSTR(H.TRAN_PARTICULAR,1,3)<>'AFT')) THEN 'CASH'
					  ELSE  '-'
				 END  CASH_TRANSACTIONS,
				 x.DELIVERY_CHANNEL_ID    
		FROM     
				  TBAADM.HTD H 
				  INNER JOIN (SELECT tran_id,tran_date,DELIVERY_CHANNEL_ID FROM tbaadm.hth WHERE  TRAN_DATE=(SELECT dc_cls_date FROM tbaadm.gct)) x ON 
				  h.tran_id=x.tran_id AND h.tran_date=x.tran_date
				  INNER JOIN tbaadm.gct ON 1=1
		WHERE    
		H.TRAN_DATE=dc_cls_date  
		AND      H.PSTD_FLG='Y';
		
		-->> RTH
		select rth.FXD_CRNCY_CODE CCY_CODE_1, rth.var_crncy_code CCY_CODE_2, 
		rth.var_crncy_units/rth.fxd_crncy_units rate,
		rth.rtlist_date RATE_DATE from TBAADM.rth rth
		inner join (select max(lchg_time) max_lchg_time , rth.FXD_CRNCY_CODE, rth.var_crncy_code,rth.rtlist_date  from TBAADM.rth
		where rth.ratecode='NOR' and rth.rtlist_date = (select DC_CLS_DATE from tbaadm.gct )  and rth.del_flg='N' 
		group by rth.FXD_CRNCY_CODE,rth.var_crncy_code,rth.rtlist_date) rth_max on rth_max.max_lchg_time=rth.lchg_time and rth_max.FXD_CRNCY_CODE=rth.FXD_CRNCY_CODE
		and RTH.VAR_CRNCY_CODE=RTH_MAX.VAR_CRNCY_CODE and RTH_MAX.RTLIST_DATE=RTH.RTLIST_DATE
		where rth.ratecode='NOR' and rth.rtlist_date =  (select DC_CLS_DATE from tbaadm.gct )   and rth.del_flg='N';
		
		UPDATE THE EXCHANGE RATE IN HTD FROM RTH
		
-->>    ATD

        SELECT *
		FROM   tbaadm.atd
		WHERE  tran_date>=(SELECT dc_cls_date-30 FROM tbaadm.gct) ;

-->>    ADT
		SELECT * FROM tbaadm.adt 
		WHERE AUDIT_DATE > = SYSDATE-3 and table_NAME = 'GAM' AND FUNC_CODE='A' AND nvl(auth_id,'!')<>'!'  
		AND instr(upper(rmks),upper('Cancelled'))=0 AND acid NOT IN (SELECT acid FROM tbaadm.alr)
		AND AUTH_BOD_DATE >= SYSDATE-3;