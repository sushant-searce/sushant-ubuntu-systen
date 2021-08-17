StructField[] TBAADM.ADT = {
					new StructField("ACID", DataTypes.StringType, true, Metadata.empty()), // 0
					new StructField("AUDIT_DATE", DataTypes.StringType, true, Metadata.empty()), // 1
					new StructField("table_NAME", DataTypes.StringType, true, Metadata.empty()), // 2
					new StructField("FUNC_CODE", DataTypes.StringType, true, Metadata.empty()), // 3
					new StructField("auth_id", DataTypes.StringType, true, Metadata.empty()), // 4
					new StructField("instr", DataTypes.StringType, true, Metadata.empty()), // 5
					new StructField("AUTH_BOD_DATE", DataTypes.StringType, true, Metadata.empty()), // 6
					new StructField("PCAS_IND", DataTypes.StringType, true, Metadata.empty()), // 7

            StructType fpVisaOutSchema = new StructType(TBAADM.ADT);



StructField[] TBAADM.GSH = {
					new StructField("GSH.DEL_FLG", DataTypes.StringType, true, Metadata.empty()), // 8
					new StructField("gl_code", DataTypes.StringType, true, Metadata.empty()), // 9
					new StructField("gl_sub_head_code", DataTypes.StringType, true, Metadata.empty()), // 10
            StructType fpVisaOutSchema = new StructType(TBAADM.GSH);


StructField[] TBAADM.CAM = {
					new StructField("ACID", DataTypes.StringType, true, Metadata.empty()), // 11
					new StructField("ACCT_STATUS", DataTypes.StringType, true, Metadata.empty()), // 12
					new StructField("ACCT_STATUS_DATE", DataTypes.StringType, true, Metadata.empty()), // 13
					new StructField("ACCT_CLS_REASON_CODE", DataTypes.StringType, true, Metadata.empty()), // 14
					new StructField("DEL_FLG", DataTypes.StringType, true, Metadata.empty()), // 15
					new StructField("ENTITY_CRE_FLG", DataTypes.StringType, true, Metadata.empty()), // 16
            StructType fpVisaOutSchema = new StructType(TBAADM.CAM);




					new StructField("MERCHANT_COUNTRY_CODE", DataTypes.StringType, true, Metadata.empty()), // 17
					new StructField("MERCHANT_CATEGORY_CODE", DataTypes.StringType, true, Metadata.empty()), // 18
					new StructField("MERCHANT_ZIP", DataTypes.StringType, true, Metadata.empty()), // 19
					new StructField("MERCHANT_STATE", DataTypes.StringType, true, Metadata.empty()), // 20
					new StructField("RPS", DataTypes.StringType, true, Metadata.empty()), // 21
					new StructField("RES", DataTypes.StringType, true, Metadata.empty()), // 22
					new StructField("UC", DataTypes.StringType, true, Metadata.empty()), // 23
					new StructField("REASON_CODE", DataTypes.StringType, true, Metadata.empty()), // 24
					new StructField("SETTLMENT_FLAG", DataTypes.StringType, true, Metadata.empty()), // 25
					new StructField("AUTH_CHAR_IND", DataTypes.StringType, true, Metadata.empty()), // 26
					new StructField("AUTH_CODE", DataTypes.StringType, true, Metadata.empty()), // 27
					new StructField("POS_TERM_CAP", DataTypes.StringType, true, Metadata.empty()), // 28
					new StructField("INT_FEE_IND", DataTypes.StringType, true, Metadata.empty()), // 29
					new StructField("CARDID_METHOD", DataTypes.StringType, true, Metadata.empty()), // 30
					new StructField("CO_FLAG", DataTypes.StringType, true, Metadata.empty()), // 31
					new StructField("POS_ENTRY_MODE", DataTypes.StringType, true, Metadata.empty()), // 32
					new StructField("CPD", DataTypes.StringType, true, Metadata.empty()), // 33
					new StructField("REIM_ATTR", DataTypes.StringType, true, Metadata.empty()), // 34
					new StructField("ISS_WORKBIN", DataTypes.StringType, true, Metadata.empty()), // 35
					new StructField("ACQ_WORKBIN", DataTypes.StringType, true, Metadata.empty()), // 36
					new StructField("CHBK_REF_NUM", DataTypes.StringType, true, Metadata.empty()), // 37
					new StructField("DOC_IND", DataTypes.StringType, true, Metadata.empty()), // 38
					new StructField("MEM_MSG_TXT", DataTypes.StringType, true, Metadata.empty()), // 39
					new StructField("SC_IND", DataTypes.StringType, true, Metadata.empty()), // 40
					new StructField("FEE_PRO_IND", DataTypes.StringType, true, Metadata.empty()), // 41
					new StructField("RES1", DataTypes.StringType, true, Metadata.empty()), // 42
					new StructField("CARD_ACCEPTOR_ID", DataTypes.StringType, true, Metadata.empty()), // 43
					new StructField("TERMINAL_ID", DataTypes.StringType, true, Metadata.empty()), // 44
					new StructField("NATIONAL_REIMFEE", DataTypes.StringType, true, Metadata.empty()), // 45
					new StructField("MOTO_IND", DataTypes.StringType, true, Metadata.empty()), // 46
					new StructField("SPL_CHBK_IND", DataTypes.StringType, true, Metadata.empty()), // 47
					new StructField("INT_TRACE_NUMBER", DataTypes.StringType, true, Metadata.empty()), // 48
					new StructField("UNATT_TERM_IND", DataTypes.StringType, true, Metadata.empty()), // 49
					new StructField("PREPAID_CARD_IND", DataTypes.StringType, true, Metadata.empty()), // 50
					new StructField("SERV_DEVFIELD", DataTypes.StringType, true, Metadata.empty()), // 51
					new StructField("AVS_RESPONSECODE", DataTypes.StringType, true, Metadata.empty()), // 52
					new StructField("AUTH_SOURCE_CODE", DataTypes.StringType, true, Metadata.empty()), // 53
					new StructField("PUR_IDENT_FORMAT", DataTypes.StringType, true, Metadata.empty()), // 54
					new StructField("ATM_ACCT_SEL", DataTypes.StringType, true, Metadata.empty()), // 55
					new StructField("INST_PAYCOUNT", DataTypes.StringType, true, Metadata.empty()), // 56
					new StructField("PUR_IDENT", DataTypes.StringType, true, Metadata.empty()), // 57
					new StructField("CASHBACK", DataTypes.StringType, true, Metadata.empty()), // 58
					new StructField("CHIP_CONDITION_CODE", DataTypes.StringType, true, Metadata.empty()), // 59
					new StructField("POS_ENV", DataTypes.StringType, true, Metadata.empty()), // 60
					new StructField("TRAN_IDENTIFIER", DataTypes.StringType, true, Metadata.empty()), // 61
					new StructField("AUTH_AMOUNT", DataTypes.StringType, true, Metadata.empty()), // 62
					new StructField("AUTH_CURR_CODE", DataTypes.StringType, true, Metadata.empty()), // 63
					new StructField("AUTH_RESP_CODE", DataTypes.StringType, true, Metadata.empty()), // 64
					new StructField("VALIDATION_CODE", DataTypes.StringType, true, Metadata.empty()), // 65
					new StructField("ETI_REASON", DataTypes.StringType, true, Metadata.empty()), // 66
					new StructField("CRS_PROCESSING_CODE", DataTypes.StringType, true, Metadata.empty()), // 67
					new StructField("CHBK_RIGHTS_IND", DataTypes.StringType, true, Metadata.empty()), // 68
					new StructField("MULTIPLE_CS_NUM", DataTypes.StringType, true, Metadata.empty()), // 69
					new StructField("MULTIPLE_CS_CNT", DataTypes.StringType, true, Metadata.empty()), // 70
					new StructField("MARKET_SAD_IND", DataTypes.StringType, true, Metadata.empty()), // 71
					new StructField("TOT_AUTH_AMOUNT", DataTypes.StringType, true, Metadata.empty()), // 72
					new StructField("INFORMATION_IND", DataTypes.StringType, true, Metadata.empty()), // 73
					new StructField("MERCHNAT_TEL_NO", DataTypes.StringType, true, Metadata.empty()), // 74
					new StructField("ADDI_DATA_IND", DataTypes.StringType, true, Metadata.empty()), // 75
					new StructField("MERCHNAT_VOL_IND", DataTypes.StringType, true, Metadata.empty()), // 76
					new StructField("RES_TCR_5", DataTypes.StringType, true, Metadata.empty()), // 77
					new StructField("TRXN_TYPE", DataTypes.StringType, true, Metadata.empty()), // 78 not initialized
					new StructField("CARD_SEQ_NUM", DataTypes.StringType, true, Metadata.empty()), // 79
					new StructField("TERMINAL_TRAN_DATE", DataTypes.StringType, true, Metadata.empty()), // 80
					new StructField("TERMINAL_CAP_PROFILE", DataTypes.StringType, true, Metadata.empty()), // 81
					new StructField("TERMINAL_CNTRY_CODE", DataTypes.StringType, true, Metadata.empty()), // 82
					new StructField("TERMINAL_SERIAL_NUM", DataTypes.StringType, true, Metadata.empty()), // 83
					new StructField("UNPRIDICT_NUM", DataTypes.StringType, true, Metadata.empty()), // 84
					new StructField("APPL_TRAN_COUNTER", DataTypes.StringType, true, Metadata.empty()), // 85
					new StructField("APPL_INTER_PROFILE", DataTypes.StringType, true, Metadata.empty()), // 86
					new StructField("CRYPTOGRAM", DataTypes.StringType, true, Metadata.empty()), // 87
					new StructField("DERIVATION_KEY_INDX", DataTypes.StringType, true, Metadata.empty()), // 88
					new StructField("CRYPTOGRAM_VER", DataTypes.StringType, true, Metadata.empty()), // 89
					new StructField("TERM_VERIFY_RESULTS", DataTypes.StringType, true, Metadata.empty()), // 90
					new StructField("CARD_VERIFY_RESULTS", DataTypes.StringType, true, Metadata.empty()), // 91
					new StructField("RES_TCR_7", DataTypes.StringType, true, Metadata.empty()), // 92
					new StructField("ISSUER_SCRIPT_RES", DataTypes.StringType, true, Metadata.empty()), // 93
					new StructField("ID", DataTypes.StringType, true, Metadata.empty()), // 94
					new StructField("FILE_DATE", DataTypes.DateType, true, Metadata.empty()), // 95
					new StructField("FILE_NAME", DataTypes.StringType, true, Metadata.empty()), // 96
					new StructField("PROCESS_DATE", DataTypes.DateType, true, Metadata.empty()), // 97
					new StructField("ORIGINAL_TRAN_DATE", DataTypes.DateType, true, Metadata.empty()), // 98
					new StructField("CHK_FLAG", DataTypes.StringType, true, Metadata.empty()), // 99
					new StructField("SETTLEMENT_DATE", DataTypes.DateType, true, Metadata.empty()), // 100 //to do
					new StructField("REJECTION_DATE", DataTypes.DateType, true, Metadata.empty()), // 101 //to do
					new StructField("MIPM_ID", DataTypes.StringType, true, Metadata.empty()), // 102
					new StructField("BRANCH_CODE", DataTypes.StringType, true, Metadata.empty()), // 103
					new StructField("ENVELOPE_NUMBER", DataTypes.StringType, true, Metadata.empty()), // 104
					new StructField("SCHEDULE_NUMBER", DataTypes.StringType, true, Metadata.empty()), // 105
					new StructField("MAKERID", DataTypes.StringType, true, Metadata.empty()), // 106
					new StructField("MAKER_DATE", DataTypes.DateType, true, Metadata.empty()), // 107 //to do
					new StructField("COMM", DataTypes.createDecimalType(20, 2), true, Metadata.empty()), // 108
					new StructField("CHECKER_DATE", DataTypes.DateType, true, Metadata.empty()), // 109
					new StructField("CHECKER_REJECTION_MSG", DataTypes.StringType, true, Metadata.empty()), // 110
					new StructField("TERM_INVOICE_NUM", DataTypes.StringType, true, Metadata.empty()), // 111
					new StructField("OUT_FILE_NAME", DataTypes.StringType, true, Metadata.empty()), // 112
					new StructField("PRT_ACNT_ID", DataTypes.StringType, true, Metadata.empty()), // 113
					new StructField("PROCESS_ID", DataTypes.StringType, true, Metadata.empty()), // 114
					new StructField("CHECKER_ID", DataTypes.StringType, true, Metadata.empty()), // 115
					new StructField("REJECTION_MSG", DataTypes.StringType, true, Metadata.empty()), // 116
					new StructField("CHRGBK_REASON", DataTypes.StringType, true, Metadata.empty()), // 117
					new StructField("NUM_OF_CHRGBK", DataTypes.StringType, true, Metadata.empty()), // 118
					new StructField("ORIG_TC", DataTypes.StringType, true, Metadata.empty()), // 119
					new StructField("LOFO", DataTypes.StringType, true, Metadata.empty()), // 120
					new StructField("TCR3_BSN_FORMAT_CODE", DataTypes.StringType, true, Metadata.empty()), // 121
					new StructField("TCR3_BSN_APP_ID", DataTypes.StringType, true, Metadata.empty()), // 122
					new StructField("TCR3_SRC_OF_FUNDS", DataTypes.StringType, true, Metadata.empty()), // 123
					new StructField("TCR3_PAY_REV_RSN_CODE", DataTypes.StringType, true, Metadata.empty()), // 124
					new StructField("TCR3_ORIG_APP_DATA", DataTypes.StringType, true, Metadata.empty()), // 125
					new StructField("UTIL_FLAG", DataTypes.StringType, true, Metadata.empty()), // 126
					new StructField("ACNT_ID", DataTypes.StringType, true, Metadata.empty()), // 127
					new StructField("BATCH_NUM1", DataTypes.StringType, true, Metadata.empty()), // 128
					new StructField("FILLER5", DataTypes.StringType, true, Metadata.empty()), // 129
					new StructField("POI_AMOUNT", DataTypes.StringType, true, Metadata.empty()), // 130
					new StructField("POI_CURR_CODE", DataTypes.StringType, true, Metadata.empty()), // 131
					new StructField("POI_CURR_EXP", DataTypes.StringType, true, Metadata.empty()), // 132
					new StructField("CURR_EXP", DataTypes.StringType, true, Metadata.empty()), // 133
					new StructField("GATEWAY_TYPE", DataTypes.StringType, true, Metadata.empty()), // 134
					new StructField("DEVICE_TYPE", DataTypes.StringType, true, Metadata.empty()), // 135
					new StructField("TECH_TYPE", DataTypes.StringType, true, Metadata.empty()), // 136
					new StructField("OUT_FILE_CTRL", DataTypes.StringType, true, Metadata.empty()), // 137
					new StructField("SWITCH_SOURCE_AMT", DataTypes.StringType, true, Metadata.empty()), // 138
					new StructField("SWITCH_CURR_EXP", DataTypes.StringType, true, Metadata.empty()), // 139
					new StructField("TCODE_04", DataTypes.StringType, true, Metadata.empty()), // 140
					new StructField("TC_QUALIFIER_04", DataTypes.StringType, true, Metadata.empty()), // 141
					new StructField("TCSN_04", DataTypes.StringType, true, Metadata.empty()), // 142
					new StructField("RESERVED_04", DataTypes.StringType, true, Metadata.empty()), // 143
					new StructField("BUS_FMT_CD", DataTypes.StringType, true, Metadata.empty()), // 144
					new StructField("DEBIT_PROD_CD", DataTypes.StringType, true, Metadata.empty()), // 145
					new StructField("CONTACT_INFO", DataTypes.StringType, true, Metadata.empty()), // 146
					new StructField("ADJ_PROCESS_IND", DataTypes.StringType, true, Metadata.empty()), // 147
					new StructField("MESS_RC", DataTypes.StringType, true, Metadata.empty()), // 148
					new StructField("SURCHAGE_AMT", DataTypes.StringType, true, Metadata.empty()), // 149
					new StructField("SUR_DRCR_IND", DataTypes.StringType, true, Metadata.empty()), // 150
					new StructField("VISA_INTR_USE", DataTypes.StringType, true, Metadata.empty()), // 151
					new StructField("FILLER1_04", DataTypes.StringType, true, Metadata.empty()), // 152
					new StructField("SUR_AMTCH_BILLCURR", DataTypes.StringType, true, Metadata.empty()), // 153
					new StructField("FILLER2_04", DataTypes.StringType, true, Metadata.empty()), // 154
					new StructField("MERCH_VERI_VAL", DataTypes.StringType, true, Metadata.empty()), // 155
					new StructField("TCSN_01", DataTypes.StringType, true, Metadata.empty()), // 156
					new StructField("TCSN_03", DataTypes.StringType, true, Metadata.empty()), // 157
					new StructField("TCSN_05", DataTypes.StringType, true, Metadata.empty()), // 158
					new StructField("TCSN_07", DataTypes.StringType, true, Metadata.empty()), // 159
					new StructField("TERM_SEQ_NO", DataTypes.StringType, true, Metadata.empty()), // 160
					new StructField("ACQ_PRIVATE_DATA", DataTypes.StringType, true, Metadata.empty()), // 161
					new StructField("AUTH_AMOUNT_05", DataTypes.StringType, true, Metadata.empty()), // 162
					new StructField("LOGIN_ID_MAKER", DataTypes.StringType, true, Metadata.empty()), // 163
					new StructField("TXN_CAT", DataTypes.StringType, true, Metadata.empty()), // 164
					new StructField("SERVICE_CODE", DataTypes.StringType, true, Metadata.empty()), // 165
					new StructField("MSG_TYPE", DataTypes.StringType, true, Metadata.empty()), // 166
					new StructField("ORG_ID", DataTypes.createDecimalType(), true, Metadata.empty()), // 167
					new StructField("OUT_FILE_CTRL_SWT", DataTypes.StringType, true, Metadata.empty()), // 168
					new StructField("ELEC_COMM_GOODS_IND", DataTypes.StringType, true, Metadata.empty()), // 169
					new StructField("PROGRAM_ID", DataTypes.StringType, true, Metadata.empty()), // 170
					new StructField("PRODUCT_ID", DataTypes.StringType, true, Metadata.empty()), // 171
					new StructField("CVV2_RESULT_CODE", DataTypes.StringType, true, Metadata.empty()), // 172
					new StructField("TCR5_BASECUR_TO_DESTCUR_EXRATE", DataTypes.StringType, true, Metadata.empty()), // 173
					new StructField("TCR5_INT_FEE_AMT", DataTypes.StringType, true, Metadata.empty()), // 174
					new StructField("TCR5_OPT_ISS_FEE_AMT", DataTypes.StringType, true, Metadata.empty()), // 175
					new StructField("TCR5_RES_1", DataTypes.StringType, true, Metadata.empty()), // 176
					new StructField("TCR5_SRCCUR_TO_BASECUR_EXRATE", DataTypes.StringType, true, Metadata.empty()), // 177
					new StructField("TCR7_CRYPTO_AMT", DataTypes.StringType, true, Metadata.empty()), // 178
					new StructField("TCR7_ISSAPP_BYTE_1", DataTypes.StringType, true, Metadata.empty()), // 179
					new StructField("TCR7_ISSAPP_BYTE_17", DataTypes.StringType, true, Metadata.empty()), // 180
					new StructField("TCR7_ISSAPP_BYTE_18_32", DataTypes.StringType, true, Metadata.empty()), // 181
					new StructField("TCR7_ISSAPP_BYTE_8", DataTypes.StringType, true, Metadata.empty()), // 182
					new StructField("TCR7_ISSAPP_BYTE_9_16", DataTypes.StringType, true, Metadata.empty()), // 183
					new StructField("CTX_ID", DataTypes.StringType, true, Metadata.empty()), // 184
					new StructField("MTX_ID", DataTypes.StringType, true, Metadata.empty()), // 185
					new StructField("FILLER1", DataTypes.StringType, true, Metadata.empty()), // 186
					new StructField("CARD_PROGRAM", DataTypes.StringType, true, Metadata.empty()), // 187
					new StructField("TOT_COMM", DataTypes.StringType, true, Metadata.empty()), // 188
					new StructField("SURCHARGE", DataTypes.StringType, true, Metadata.empty()), // 189
					new StructField("PAY_FAC_ID", DataTypes.StringType, true, Metadata.empty()), // 190
					new StructField("SUB_MRCH_ID", DataTypes.StringType, true, Metadata.empty()), // 191
					new StructField("BANK_SOURCE", DataTypes.StringType, true, Metadata.empty()), // 192
					new StructField("SuccessFlag", DataTypes.BooleanType, true, Metadata.empty()) };

			StructType fpVisaOutSchema = new StructType(fpVisaOutschemaFields);