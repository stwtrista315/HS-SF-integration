#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Created by: Abdul Capstone Fall 2023
Date Created: 11/12/2023

'''
import sys
import PrivateKeys as fetchkeys
import pandas as pd
import Functions as custfunc
import requests
import DB_Connection as connection
import copy
from datetime import datetime

# CONNECT TO DATABASE SERVER
# DB Connection Setup
mydb, mySchema = connection.getDatabaseConnection()
mycursor = mydb.cursor(buffered=True)


def run_job_two():
    try:
        url, authHeader = fetchkeys.access_variables()  # function call to fetch the url, authHeader, api key and auth token
        
        # CONNECT TO DATABASE SERVER
        # DB Connection Setup
        mydb, mySchema = connection.getDatabaseConnection()
        mycursor = mydb.cursor(buffered=True)

        # STAGE_1

        # Lookup table required for student data transfer START

        roles_columns = ['hs_id', 'hs_name']

        user_roles_df = custfunc.create_lookup_table(mySchema, mydb, "hs_student_user_role", roles_columns)
        group_df = custfunc.create_lookup_table(mySchema, mydb, 'hs_student_group', roles_columns)
        program_df = custfunc.create_lookup_table(mySchema, mydb, 'hs_program', roles_columns)
        campus_df = custfunc.create_lookup_table(mySchema, mydb, 'hs_campus', roles_columns)
        fourplusone_df = custfunc.create_lookup_table(mySchema, mydb, 'hs_four_plus_one_programs', roles_columns)
        gradterm_df = custfunc.create_lookup_table(mySchema, mydb, 'hs_graduation_term', roles_columns)
        workauth_df = custfunc.create_lookup_table(mySchema, mydb, 'hs_work_authorization', roles_columns)
        ethnicity_df = custfunc.create_lookup_table(mySchema, mydb, 'hs_ethnicity', roles_columns)

        # Lookup table required for student data transfer END

        # print(user_roles_df)
        # print(group_df)
        # print(program_df)
        # print(campus_df)
        # print(fourplusone_df)
        # print(gradterm_df)
        # print(workauth_df)
        # print(ethnicity_df)

        custom_attr_map = {
             # Student Custom Attributes in student summary
            'last_event_registration' : 'custom_attribute_10888805125292',
            'last_event_attendance' : 'custom_attribute_10888805125293',
            'last_coaching_registration' : 'custom_attribute_10888805125294',
            'last_coaching_attendance' : 'custom_attribute_10888805125295',
            'last_job_application' : 'custom_attribute_10888805125296',
            'resume_approval_status' : 'custom_attribute_10888805126796',
            'graduation_date' : 'custom_attribute_10888805127136',
            'job_acceptance_deadline' : 'custom_attribute_10888805127137',
            'last_coaching_type' : 'custom_attribute_10888805126797',
            'last_note_type' : 'custom_attribute_10888805126798',
            
            # Student Custom Attributes in SIS/ERX
            'mobile_phone' : 'custom_attribute_1009',
            '4+1_grad_program' : 'custom_attribute_3500',
            'cohort' : 'custom_attribute_1871',
            'campus' : 'custom_attribute_1872',
            'quest_program' : 'custom_attribute_917',
            'security_clearance' : 'custom_attribute_918',
            'is_sponsored' : 'custom_attribute_10200',
            'start_term' : 'custom_attribute_10888805113030',
            'transfer_student' : 'custom_attribute_10888805113026',
            'transfer_type' : 'custom_attribute_10888805113028',
            'we_chat_id' : 'custom_attribute_10888805111835',
            'primary_ug_school' : 'custom_attribute_10888805112961',
            'ug_grad_term' : 'custom_attribute_10888805113011',
            'contact_id' : 'custom_attribute_10888805131237'
            }
        
        # --- Set Modify Date for Company from last run
        mycursor = mydb.cursor(buffered=True)
        mycursor.execute(
            "SELECT max(timestamp) FROM " + mySchema + ".job_log WHERE job_name = 'HS_Student_Step_2' and category = 'Start' "
                                                       "and source = 'Hiresmith' and status = 'Success';")
        modify_date_student = str(mycursor.fetchall()[0][0]).replace(" ", "T")
        
        #For debugging
        #modify_date_student = '2023-11-21T10:30:15' # - Job 2 debugging Old-2023-11-07T15:59:19

        if str(modify_date_student) != 'None':
            script_student_fetch = "SELECT * FROM  " + mySchema + ".sf_student_view WHERE timestamp >=" 
            mycursor.execute(script_student_fetch + '\'' + modify_date_student)
        else:
            script_student_fetch = "SELECT * FROM  " + mySchema + ".sf_student_view"
            mycursor.execute(script_student_fetch + ';')

        student_sql_fetch = mycursor.fetchall()

        default_df_student = {
            "StudentClubs": [], "RoleId": None, "StudentGroups": [], "FullName": None, "IsLgbtq": None, 
            "SelfIdentifiedGenderDescription": None, "ParentEducationLevelId": None, "ParentEducationLevelName": None,
            "DoesParentHaveJD": None, "AbaGraduateId": None, "IsMultipleEnrollmentLinkedAccount": None, 
            "OutcomeStatusInternship": {"Name": None,"AttributeId": None,"Id": None},
            "OutcomeStatusPostGraduation": {"Name": None,"AttributeId": None,"Id": None},
            "ReportingCategoryMbaCseaPostGraduation": {"Name": None,"AttributeId": None,"Id": None},
            "ReportingCategoryMbaCseaInternship": {"Name": None, "AttributeId": None,"Id": None},
            "IsWorkStudyEligible": None, "FirstName": None, "MiddleName": None, "LastName": None,
            "EmailAddress": None, "GraduationYearId": None, "GraduationClass": None, "GraduationTerm": None,
            "StudentId": None, "Id": None, "GenderDescriptionIds": [], "GenderDescriptionNames": [],
            "Addresses": [], "IsAlumni": None, "IncludeInResumeBook": None, "JoinDate": None, "IsEnrolled": None,
            "LinkedInProfileUrl": None, "IsTransferStudent": None, "DeclineToStateIsTransferStudent": None,
            "HasPhoto": None, "AssignedAdvisor": None, "AssignedAdvisor2": None, "AssignedAdvisor3": None,
            "AssignedAdvisor4": None, "AssignedAdvisor5": None, "SubInfoDisplay": None,
            "YearsExperience": {"AttributeId": None, "Id": None},
            "ConsolidatedYearsExperience": {"AttributeId": None, "Id": None},
            "CountryOfCitizenship1": {"AttributeId": None,"Id": None},
            "CountryOfCitizenship2": {"AttributeId": None, "Id": None},
            "CountryOfCitizenship": {"AttributeId": None, "Id": None},
            "DualCountryOfCitizenship": {"AttributeId": None, "Id": None},
            "PreferredConsolidatedIndustry": {"AttributeId": None, "Id": None},
            "PreferredConsolidatedIndustry2": {"AttributeId": None, "Id": None},
            "PreferredConsolidatedIndustry3": {"AttributeId": None, "Id": None},
            "PreferredConsolidatedIndustry4": {"AttributeId": None, "Id": None},
            "PreferredConsolidatedIndustry5": {"AttributeId": None, "Id": None},
            "PreferredConsolidatedJobFunction": {"AttributeId": None, "Id": None},
            "PreferredConsolidatedJobFunction2": {"AttributeId": None, "Id": None},
            "PreferredConsolidatedJobFunction3": {"AttributeId": None, "Id": None},
            "PreferredConsolidatedJobFunction4": {"AttributeId": None, "Id": None},
            "PreferredConsolidatedJobFunction5": {"AttributeId": None, "Id": None},
            "PreferredCountry": {"AttributeId": None, "Id": None},
            "PreferredCountry2": {"AttributeId": None, "Id": None},
            "PreferredCountry3": {"AttributeId": None, "Id": None},
            "PreferredCountry4": {"AttributeId": None, "Id": None},
            "PreferredCountry5": {"AttributeId": None, "Id": None},
            "PreferredCity": {"AttributeId": None, "Id": None},
            "PreferredCity2": {"AttributeId": None, "Id": None},
            "PreferredCity3": {"AttributeId": None, "Id": None},
            "PreferredCity4": {"AttributeId": None, "Id": None},
            "PreferredCity5": {"AttributeId": None, "Id": None},
            "Section": {"AttributeId": None, "Id": None},
            "Program": {"Name": "MS in Information Systems", "AttributeId": None, "Id": None},
            "MbaConcentration1": {"AttributeId": None, "Id": None},
            "MbaConcentration2": {"AttributeId": None, "Id": None},
            "GmatScore": {"AttributeId": None, "Id": None},
            "UndergradMajor": {"AttributeId": None, "Id": None},
            "UndergradMajor2": {"AttributeId": None, "Id": None},
            "UndergradMajor3": {"AttributeId": None, "Id": None},
            "UndergradMajor4": {"AttributeId": None, "Id": None},
            "UndergradMajor5": {"AttributeId": None, "Id": None},
            "ConsolidatedMajor1": {"AttributeId": None, "Id": None},
            "ConsolidatedMajor2": {"AttributeId": None, "Id": None},
            "ConsolidatedMajor3": {"AttributeId": None, "Id": None},
            "ConsolidatedMajor4": {"AttributeId": None, "Id": None},
            "ConsolidatedMajor5": {"AttributeId": None, "Id": None},
            "Ethnicity1": {"AttributeId": None, "Id": None},
            "Ethnicity2": {"AttributeId": None, "Id": None},
            "Placeability": {"AttributeId": None, "Id": None},
            "Engagement": {"AttributeId": None, "Id": None},
            "FirstLanguageSpoken": {"AttributeId": None, "Id": None},
            "SecondLanguageSpoken": {"AttributeId": None, "Id": None},
            "ThirdLanguageSpoken": {"AttributeId": None, "Id": None},
            "FirstLanguageWritten": {"AttributeId": None, "Id": None},
            "SecondLanguageWritten": {"AttributeId": None, "Id": None},
            "ThirdLanguageWritten": {"AttributeId": None, "Id": None},
            "LanguageSpoken1": {"AttributeId": None, "Id": None},
            "LanguageSpoken2": {"AttributeId": None, "Id": None},
            "LanguageSpoken3": {"AttributeId": None, "Id": None},
            "LanguageWritten1": {"AttributeId": None, "Id": None},
            "LanguageWritten2": {"AttributeId": None, "Id": None},
            "LanguageWritten3": {"AttributeId": None, "Id": None},
            "WorkAuthorization": {"AttributeId": None, "Id": None},
            "ConsolidatedWorkAuthorization": {"AttributeId": None, "Id": None},
            "FirstCountryOfWorkAuthorization": {"AttributeId": None, "Id": None},
            "SecondCountryOfWorkAuthorization": {"AttributeId": None, "Id": None},
            "ThirdCountryOfWorkAuthorization": {"AttributeId": None, "Id": None},
            "CountryOfWorkAuthorization1": {"AttributeId": None, "Id": None},
            "CountryOfWorkAuthorization2": {"AttributeId": None, "Id": None},
            "CountryOfWorkAuthorization3": {"AttributeId": None, "Id": None},
            "CustomAttributeValues": {
                "custom_attribute_804": None,
                "custom_attribute_10888805111015": None,
                "custom_attribute_10888805129916": None,
                "custom_attribute_10888805123971": None,
                custom_attr_map['start_term']: None,
                "custom_attribute_10888805129917": None,
                "custom_attribute_10888805131236": None,
                custom_attr_map['primary_ug_school']: None,
                custom_attr_map['contact_id']: None,
                custom_attr_map['transfer_student']: None,
                custom_attr_map['transfer_type']: None,
                "custom_attribute_10888805133122": None,
                custom_attr_map['cohort']: None,
                custom_attr_map['campus']: None,
                custom_attr_map['mobile_phone']: None,
                custom_attr_map['we_chat_id']: None,
                custom_attr_map['ug_grad_term']: None,
                custom_attr_map['quest_program']: None,
                custom_attr_map['4+1_grad_program']: None,
                custom_attr_map['security_clearance']: None,
                custom_attr_map['last_event_registration']: None,
                custom_attr_map['last_event_attendance']: None,
                custom_attr_map['last_coaching_registration']: None,
                custom_attr_map['last_coaching_attendance']: None,
                custom_attr_map['last_job_application']: None,
                custom_attr_map['resume_approval_status']: None,
                "custom_attribute_10888805133216": None,
                custom_attr_map['graduation_date']: None,
                custom_attr_map['job_acceptance_deadline']: None,
                custom_attr_map['last_coaching_type']: None,
                custom_attr_map['last_note_type']: None
            }
        }
     
        default_df_student = {
            "StudentId": None, # Contact: UID Number Text
            "FirstName": None, # Contact: First Name	
            "MiddleName": None, # Contact: Middle Name	
            "LastName": None, # Contact: Last Name
			# Note: Contact: Name Suffix ?
            "PreferredEmailAddress": None, # Contact: Email; It is called Alternate Email Address on HS Portal
            "EmailAddress": None, # Contact: UMD Email Address	
            "LinkedInProfileUrl": None, # Contact: Linkedin URL	
            
            "Gender": None,	# Contact: Gender
			"SelfIdentifiedGenderDescription": None,	
            "GenderDescriptionIds": [], 
            "GenderDescriptionNames": [],
            
            "Ethnicity1": {"AttributeId": None, "Id": None}, # Contact: Race	
            "Ethnicity2": {"AttributeId": None, "Id": None}, # Contact: Hispanic	
            "Program": {"Name": None, "AttributeId": None, "Id": None}, # Program Degree
            # Note: Program Type gets combined with Program Degree
            # Note: Level of Study ?
            # Note: Military Status	Military Branch ?
            
            "CountryOfCitizenship1": {"AttributeId": None,"Id": None}, # Contact: Country of Citizenship + Contact: Citizenship Status
            "CountryOfCitizenship2": {"AttributeId": None, "Id": None},
            "CountryOfCitizenship": {"AttributeId": None, "Id": None},
            "DualCountryOfCitizenship": {"AttributeId": None, "Id": None},
			
            "YearsExperience": {"AttributeId": None, "Id": None}, # Months Worked
            "WorkAuthorization": {"AttributeId": None, "Id": None}, # Admissions Visa Status
            
            "StudentClubs": [], "RoleId": None, "StudentGroups": [], 
            "FullName": None, # Note: Can be constructed by FirstName+LastName
            "IsLgbtq": None, 
            "ParentEducationLevelId": None, 
			"ParentEducationLevelName": None,
            "DoesParentHaveJD": None, 
			"AbaGraduateId": None, 
			"IsMultipleEnrollmentLinkedAccount": None, # Note: would have to construct a logic to deal with multiple accounts o/p: Yes/No
            "OutcomeStatusInternship": {"Name": None,"AttributeId": None,"Id": None},
            "OutcomeStatusPostGraduation": {"Name": None,"AttributeId": None,"Id": None},
            "ReportingCategoryMbaCseaPostGraduation": {"Name": None,"AttributeId": None,"Id": None},
            "ReportingCategoryMbaCseaInternship": {"Name": None, "AttributeId": None,"Id": None},
            "IsWorkStudyEligible": None, 
            "GraduationYearId": None, "GraduationClass": None, 
            "GraduationTerm": None, 
            "Id": None, # Note: Only available for pre-existing students
            "Addresses": [], "IsAlumni": None, 
			"IncludeInResumeBook": None, 
			"JoinDate": None, 
			"IsEnrolled": None,
            "IsTransferStudent": None, 
			"DeclineToStateIsTransferStudent": None,
            "HasPhoto": None, "AssignedAdvisor": None, "AssignedAdvisor2": None, "AssignedAdvisor3": None,
            "AssignedAdvisor4": None, "AssignedAdvisor5": None, "SubInfoDisplay": None,
            "ConsolidatedYearsExperience": {"AttributeId": None, "Id": None}, # Note: Can be constructed out of YearsExperience
            "PreferredConsolidatedIndustry": {"AttributeId": None, "Id": None},
            "PreferredConsolidatedIndustry2": {"AttributeId": None, "Id": None},
            "PreferredConsolidatedIndustry3": {"AttributeId": None, "Id": None},
            "PreferredConsolidatedIndustry4": {"AttributeId": None, "Id": None},
            "PreferredConsolidatedIndustry5": {"AttributeId": None, "Id": None},
            "PreferredConsolidatedJobFunction": {"AttributeId": None, "Id": None},
            "PreferredConsolidatedJobFunction2": {"AttributeId": None, "Id": None},
            "PreferredConsolidatedJobFunction3": {"AttributeId": None, "Id": None},
            "PreferredConsolidatedJobFunction4": {"AttributeId": None, "Id": None},
            "PreferredConsolidatedJobFunction5": {"AttributeId": None, "Id": None},
            "PreferredCountry": {"AttributeId": None, "Id": None},
            "PreferredCountry2": {"AttributeId": None, "Id": None},
            "PreferredCountry3": {"AttributeId": None, "Id": None},
            "PreferredCountry4": {"AttributeId": None, "Id": None},
            "PreferredCountry5": {"AttributeId": None, "Id": None},
            "PreferredCity": {"AttributeId": None, "Id": None},
            "PreferredCity2": {"AttributeId": None, "Id": None},
            "PreferredCity3": {"AttributeId": None, "Id": None},
            "PreferredCity4": {"AttributeId": None, "Id": None},
            "PreferredCity5": {"AttributeId": None, "Id": None},
            "Section": {"AttributeId": None, "Id": None},
            "MbaConcentration1": {"AttributeId": None, "Id": None},
            "MbaConcentration2": {"AttributeId": None, "Id": None},
            "GmatScore": {"AttributeId": None, "Id": None},
            "UndergradMajor": {"AttributeId": None, "Id": None},
            "UndergradMajor2": {"AttributeId": None, "Id": None},
            "UndergradMajor3": {"AttributeId": None, "Id": None},
            "UndergradMajor4": {"AttributeId": None, "Id": None},
            "UndergradMajor5": {"AttributeId": None, "Id": None},
            "ConsolidatedMajor1": {"AttributeId": None, "Id": None},
            "ConsolidatedMajor2": {"AttributeId": None, "Id": None},
            "ConsolidatedMajor3": {"AttributeId": None, "Id": None},
            "ConsolidatedMajor4": {"AttributeId": None, "Id": None},
            "ConsolidatedMajor5": {"AttributeId": None, "Id": None},
            "Placeability": {"AttributeId": None, "Id": None},
            "Engagement": {"AttributeId": None, "Id": None},
            "FirstLanguageSpoken": {"AttributeId": None, "Id": None},
            "SecondLanguageSpoken": {"AttributeId": None, "Id": None},
            "ThirdLanguageSpoken": {"AttributeId": None, "Id": None},
            "FirstLanguageWritten": {"AttributeId": None, "Id": None},
            "SecondLanguageWritten": {"AttributeId": None, "Id": None},
            "ThirdLanguageWritten": {"AttributeId": None, "Id": None},
            "LanguageSpoken1": {"AttributeId": None, "Id": None},
            "LanguageSpoken2": {"AttributeId": None, "Id": None},
            "LanguageSpoken3": {"AttributeId": None, "Id": None},
            "LanguageWritten1": {"AttributeId": None, "Id": None},
            "LanguageWritten2": {"AttributeId": None, "Id": None},
            "LanguageWritten3": {"AttributeId": None, "Id": None},
            "ConsolidatedWorkAuthorization": {"AttributeId": None, "Id": None},
            "FirstCountryOfWorkAuthorization": {"AttributeId": None, "Id": None},
            "SecondCountryOfWorkAuthorization": {"AttributeId": None, "Id": None},
            "ThirdCountryOfWorkAuthorization": {"AttributeId": None, "Id": None},
            "CountryOfWorkAuthorization1": {"AttributeId": None, "Id": None},
            "CountryOfWorkAuthorization2": {"AttributeId": None, "Id": None},
            "CountryOfWorkAuthorization3": {"AttributeId": None, "Id": None},
            "CustomAttributeValues": {
                custom_attr_map['we_chat_id']: None, # Contact: We Chat ID
                custom_attr_map['campus']: None, # Program Campus
                custom_attr_map['start_term']: None, # Term
                custom_attr_map['contact_id']: None, # Contact: Contact ID
				
                "custom_attribute_804": None,
                "custom_attribute_10888805111015": None,
                "custom_attribute_10888805129916": None,
                "custom_attribute_10888805123971": None,
                "custom_attribute_10888805129917": None,
                "custom_attribute_10888805131236": None,
                custom_attr_map['primary_ug_school']: None,
                custom_attr_map['transfer_student']: None,
                custom_attr_map['transfer_type']: None,
                "custom_attribute_10888805133122": None,
                custom_attr_map['cohort']: None,
                custom_attr_map['mobile_phone']: None,
                custom_attr_map['ug_grad_term']: None,
                custom_attr_map['quest_program']: None,
                custom_attr_map['4+1_grad_program']: None,
                custom_attr_map['security_clearance']: None,
                custom_attr_map['last_event_registration']: None,
                custom_attr_map['last_event_attendance']: None,
                custom_attr_map['last_coaching_registration']: None,
                custom_attr_map['last_coaching_attendance']: None,
                custom_attr_map['last_job_application']: None,
                custom_attr_map['resume_approval_status']: None,
                "custom_attribute_10888805133216": None,
                custom_attr_map['graduation_date']: None,
                custom_attr_map['job_acceptance_deadline']: None,
                custom_attr_map['last_coaching_type']: None,
                custom_attr_map['last_note_type']: None
            }
        }

        # Create new log entry for job 2 start
        mycursor.execute(
            "INSERT INTO  " + mySchema + ".job_log (job_name, source, category, status) VALUES ('HS_Student_Step_2', "
                                         "'Hiresmith', 'Start', 'Success');")
        mydb.commit()

        # ADD Values for Company fetched from Intermediate Database to Data Dictionary Format
        final_student_put = {}
        final_student_post = {}

        script_student_error_log_columns = []

        script_student_error_log = custfunc.insert_sql(mySchema, "error_log_student", script_student_error_log_columns, False)
        
        for x in student_sql_fetch:
            student_view_sql_api_map = {
                #TODO
                'status' : x['random number'],
                'timestamp' : x['status# + 1']
            }

            try:
                if student_view_sql_api_map['status'] == 'Insert':
                    pass

                else:
                    pass

            except BaseException as e:
                pass

        mycursor.execute(
            "INSERT INTO  " + mySchema + ".job_log (job_name, source, category, status) VALUES ('HS_Student_Step_2', "
                                         "'Hiresmith', 'End', 'Success');")
        mydb.commit()

    except BaseException as e:
        exception_type, exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno

        print("Exception type: ", exception_type)
        print("File name: ", filename)
        print("Line number: ", line_number)
        print("Error code:  ", e)

    ## END OF JOB 2

# ##COMPANY STEP 2
# mycursor.execute("SELECT MAX(timestamp) FROM " + mySchema + ".job_log WHERE job_name = 'HS_Company_Step_2' AND category = 'End';")
# CompanyEndTime = mycursor.fetchall()

# mycursor.execute("SELECT MAX(timestamp) FROM " + mySchema + ".job_log WHERE job_name = 'HS_Company_Step_2' AND category = 'Start';")
# CompanyStartTime = mycursor.fetchall()

# ##CONTACT STEP 2
# mycursor.execute("SELECT MAX(timestamp) FROM " + mySchema + ".job_log WHERE job_name = 'HS_Contact_Step_2' AND category = 'End';")
# ContactEndTime = mycursor.fetchall()

# mycursor.execute("SELECT MAX(timestamp) FROM " + mySchema + ".job_log WHERE job_name = 'HS_Contact_Step_2' AND category = 'Start';")
# ContactStartTime = mycursor.fetchall()

# print(CompanyEndTime)
# print(CompanyStartTime)
# print(ContactEndTime)
# print(ContactStartTime)

# ## Executing the code if below condition is matched
# if CompanyEndTime >= CompanyStartTime and ContactEndTime >= ContactStartTime:
#     run_job_two()

run_job_two()