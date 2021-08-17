q1.                 WHEN GAM.SCHM_TYPE = 'TDA' THEN
								  CASE
								 WHEN TAM.MATURITY_DATE <= (SELECT DC_CLS_DATE FROM TBAADM.GCT) AND
									  GAM.ACCT_CLS_DATE IS NULL THEN
								  'OVERDUE'


q2.                 FROM (SELECT 
                        .
                        .
                        .

									   CASE
										 WHEN (SELECT DC_CLS_DATE FROM TBAADM.GCT) >
                                            .
                                            .
                                            .

                                         FROM C_GAM GAM


q3.                  
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


q4.                 LEFT JOIN TBAADM.GEM ON GAM.ACID = GEM.ACID


q5.                 LEFT JOIN tbaadm.lrp p ON gam.acid=p.acid AND p.entity_cre_flg='Y' AND p.del_flg='N' /* MODIFICATION */
								 WHERE GAM.ACCT_OWNERSHIP <> 'O'
								   AND GAM.DEL_FLG = 'N'
								   AND GAM.ENTITY_CRE_FLG = 'Y') GAM


q6.                 INNER JOIN (SELECT SCHM_CODE, ACCT_PREFIX, SCHM_DESC, PRODUCT_CONCEPT
									   FROM TBAADM.GSP
									  WHERE DEL_FLG = 'N') GSP ON GAM.SCHM_CODE = GSP.SCHM_CODE


                                

                    INNER JOIN (SELECT DISTINCT GL_CODE, GL_SUB_HEAD_CODE
									   FROM TBAADM.GSH
									  WHERE GSH.DEL_FLG = 'N') GSH ON GAM.GL_SUB_HEAD_CODE =
																	  GSH.GL_SUB_HEAD_CODE







q7.                 LEFT JOIN (SELECT A.ENTITY_ID,
											.
                                            .
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


q8.                 LEFT JOIN (SELECT ACID,
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


q9.                   LEFT JOIN (SELECT DISTINCT REF_CODE, REF_DESC ACCT_CLS_REASON_DESC
									   FROM TBAADM.RCT
									  WHERE REF_REC_TYPE = 'GX') RCT_CAM ON CAM.ACCT_CLS_REASON_CODE =
																			REF_CODE


q10.                LEFT JOIN TBAADM.GAC ON GAM.ACID = GAC.ACID

q11.                LEFT JOIN (SELECT DISTINCT SOL_ID, SOL_DESC BRANCH_NAME
									   FROM TBAADM.SOL
									  WHERE DEL_FLG = 'N'
										AND BANK_CODE = '176') SOL ON GAM.SOL_ID = SOL.SOL_ID

q12.                 LEFT JOIN (SELECT REF_CODE, REF_DESC
									   FROM TBAADM.RCT
									  WHERE REF_REC_TYPE = '38') RCT_38 ON RCT_38.REF_CODE =
																		   GAC.NATURE_OF_ADVN

q13.                LEFT JOIN (SELECT ACID,
											CLOSE_ON_MATURITY_FLG,
                                            .
                                            .
                                            .
											REPAYMENT_ACID
									   FROM TBAADM.TAM) TAM ON GAM.ACID = TAM.ACID

q14.                LEFT JOIN (SELECT REF_CODE, REF_DESC
									   FROM TBAADM.RCT
									  WHERE REF_REC_TYPE = 'AF') RCT_AF ON GAC.FREE_CODE_5 =
																		   RCT_AF.REF_CODE


q15.                LEFT JOIN (SELECT REF_CODE, REF_DESC
									   FROM TBAADM.RCT
									  WHERE REF_REC_TYPE = '21') RCT_OC ON GAC.ACCT_OCCP_CODE =
																		   RCT_OC.REF_CODE


q16.                LEFT JOIN (SELECT REF_CODE, REF_DESC
									   FROM TBAADM.RCT
									  WHERE REF_REC_TYPE = '27') RCT_27 ON GAM.MODE_OF_OPER_CODE =
																		   RCT_27.REF_CODE


q17.                LEFT JOIN (SELECT REF_CODE, REF_DESC
									   FROM TBAADM.RCT
									  WHERE REF_REC_TYPE = 'CN') RCT_CN ON GAC.FREE_TEXT_10 =
																		   RCT_CN.REF_CODE


q18.                LEFT JOIN (SELECT REF_CODE, REF_DESC
									   FROM TBAADM.RCT
									  WHERE REF_REC_TYPE = '31') RCT_31 ON GAM.FREZ_REASON_CODE =
																		   RCT_31.REF_CODE


q19.                LEFT JOIN (SELECT ENTITY_ID,
											FREE_CODE_20,
											.
                                            .
                                            .
											FREE_CODE_1,
											FREE_CODE_43
									   FROM CFCM
									  WHERE CFCM.ENTITY_TYPE = 'A') CFCM ON GAM.FORACID =
																			CFCM.ENTITY_ID


q20.                LEFT JOIN TBAADM.LA_SAM ON GAM.ACID = LA_SAM.ACID


q21.                LEFT JOIN (SELECT A.ACID,
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


q22.                LEFT JOIN (SELECT ACID,
											.
                                            .
                                            .
                                            CRFILE_REF_ID,
											CASE
											  WHEN EI_SCHM_FLG = 'Y' THEN
											   'EI'
											  ELSE
											   'NON_EI'
											END AS EMI_TYPE,
											DMD_SATISFY_MTHD AS PAYMENT_METHOD
									   FROM TBAADM.LAM) LAM ON GAM.ACID = LAM.ACID


q23.                LEFT JOIN (SELECT REF_CODE, REF_DESC
									   FROM TBAADM.RCT
									  WHERE REF_REC_TYPE = 'EY') RCT_EY ON LAM.PAYOFF_REASON_CODE =
																		   RCT_EY.REF_CODE



q24.                LEFT JOIN (SELECT B2K_ID, MAIN_CLASSIFICATION_USER FROM TBAADM.ACD) ACD ON GAM.ACID =
																									 ACD.B2K_ID


q25.                LEFT JOIN (SELECT FORACID, DEBIT_CARD FROM C_DBC) C_DBC ON GAM.FORACID =
																					 C_DBC.FORACID


q26.                LEFT JOIN (SELECT LIMIT_B2KID, LIM_EXP_DATE, LIMIT_REVIEW_DATE
									   FROM TBAADM.LLT) LLT ON GAM.LIMIT_B2KID = LLT.LIMIT_B2KID


q27.                LEFT JOIN (SELECT REF_CODE, REF_DESC
									   FROM TBAADM.RCT
									  WHERE REF_REC_TYPE = 'AI') RCT_AI ON GAC.FREE_CODE_8 =
																		   RCT_AI.REF_CODE


q28.                LEFT JOIN (SELECT ACID,
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


q29.                LEFT JOIN (SELECT ENTITY_ID, NEXT_PEG_REVIEW_DATE
									   FROM TBAADM.EIT
									  WHERE NEXT_PEG_REVIEW_DATE IS NOT NULL) PR ON GAM.ACID =
																					PR.ENTITY_ID


q30.                LEFT JOIN (SELECT CUSTID,
											
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




q31.
q32.
q33.
q34.
q35.

