#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Created by: Abdul Capstone Fall 2023
Date Created: 11/12/2023

'''
# Imports and Initializations
import sys
import PrivateKeys as fetchkeys
import pandas as pd
import Functions as custfunc
import requests
import DB_Connection as connection
import copy
from datetime import datetime
import os 
url, authHeader = fetchkeys.access_variables()  # function call to fetch the url, authHeader, api key and auth token
        
# CONNECT TO DATABASE SERVER
# Database Connection
mydb, mySchema = connection.getDatabaseConnection()
mycursor = mydb.cursor(buffered=True)

# STAGE_1
print(os.getcwd())

# Creating Lookup Tables
#load the different MAPPING FILES required
year_experience_mapping = pd.read_csv('DEV - Tharun\ocs\Scripts\Production\HS-SFInterface\mapping_csv_files\Year_experience_codes_12T.csv')
program_mapping = pd.read_csv('DEV - Tharun\ocs\Scripts\Production\HS-SFInterface\mapping_csv_files\Program_Mappings_for_input.csv')
control_file_df = pd.read_csv('DEV - Tharun\ocs\Scripts\Production\HS-SFInterface\mapping_csv_files\ControlFile.csv')
veteran_mapping = pd.read_csv('DEV - Tharun\ocs\Scripts\Production\HS-SFInterface\mapping_csv_files\Veteran.csv')
workauth_mapping = pd.read_csv('DEV - Tharun\ocs\Scripts\Production\HS-SFInterface\mapping_csv_files\WorkAuthMapping.csv')
country_ids = pd.read_csv('DEV - Tharun\ocs\Scripts\Production\HS-SFInterface\mapping_csv_files\Country_id.csv')

# Lookup table required for student data transfer START

roles_columns = ['hs_id', 'hs_name', 'sf_name']

user_roles_df = custfunc.create_lookup_table(mySchema, mydb, "hs_student_user_role", roles_columns)
student_group_df = custfunc.create_lookup_table(mySchema, mydb, 'hs_student_group', roles_columns)
program_df = custfunc.create_lookup_table(mySchema, mydb, 'hs_program', roles_columns)
campus_df = custfunc.create_lookup_table(mySchema, mydb, 'hs_campus', roles_columns)
fourplusone_df = custfunc.create_lookup_table(mySchema, mydb, 'hs_four_plus_one_programs', roles_columns)
gradterm_df = custfunc.create_lookup_table(mySchema, mydb, 'hs_graduation_term', roles_columns)
workauth_df = custfunc.create_lookup_table(mySchema, mydb, 'hs_work_authorization', roles_columns)
ethnicity_df = custfunc.create_lookup_table(mySchema, mydb, 'hs_ethnicity', roles_columns)
print(student_group_df)

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

if str(modify_date_student) != 'None':
    script_student_fetch = "SELECT * FROM  " + mySchema + ".student_view WHERE timestamp >= '" + modify_date_student + "'"
    mycursor.execute(script_student_fetch)

else:
    script_student_fetch = "SELECT * FROM  " + mySchema + ".student_view"
    mycursor.execute(script_student_fetch + ';')

student_sql_fetch = mycursor.fetchall()

# Data Preparation
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
    
    "StudentClubs": [], "RoleId": None, 
    "StudentGroups": [], 
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
    "GraduationYearId": None, 
    "GraduationClass": None, 
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

#Create new log entry for job 2 start
mycursor.execute(
    "INSERT INTO  " + mySchema + ".job_log (job_name, source, category, status) VALUES ('HS_Student_Step_2', "
                                    "'Hiresmith', 'Start', 'Success');")
mydb.commit()

# ADD Values for Contacts fetched from Intermediate Database to Data Dictionary Format
script_student_error_log_columns = [
    "contact_id", "uid", "first_name", "middle_name", "last_name", "email_address",
    "student_groups", "program", "start_term", "grad_term","grad_class"
]

script_student_error_log = custfunc.insert_sql(mySchema, "error_log_student", script_student_error_log_columns, False)

final_student_put = {}
final_student_post = {}

print(student_sql_fetch)

#Looping Through SQL Results
for y in student_sql_fetch:
    # Mapping API fields to SQL Result set
    student_view_sql_api_map = {
        'sf_contact_id' : y[0],
        'uid': y[1],
        'first_name' : y[2],
        'middle_name' : y[3],
        'last_name' : y[4],
        'name_prefix' : y[5],
        'name_suffix' : y[6],
        'email' : y[7],
        'alternate_email' : y[8],
        'gender' : y[9],
        'country_of_citizenship' : y[10],
        'citz_visa' : y[11],
        'has_dual_citizenship' : y[12],
        'dual_citizenship_country' : y[13],
        'ethnicity_1' : y[14],
        'ethnicity_2' : y[15],
        'contact_hispanic' : y[16],
        'linkedin_url' : y[17],
        'primary_UG_school' : y[18],
        'mobile_phone' : y[19],
        'home_phone_formatted' : y[20],
        'wechat_id' : y[21],
        'military_status' : y[22],
        'military_branch' : y[23],
        'first_generation_students' : y[24],
        'hs_student_id' : y[25],
        'program' : y[26],
        'program_type' : y[27],
        'start_term' : y[28],
        'last_registered_term' : y[29],
        'graduation_term' : y[30],
        'graduation_clearance' : y[31],
        'undergraduate_major' : y[32],
        'undergraduate_major_2' : y[33],
        'transfer_type' : y[34],
        'cohort' : y[35],
        'campus' : y[36],
        'level_of_study' : y[37],
        'quest_program' : y[38],
        'four_plus_one_grad_program' : y[39],
        'shady_grove_ind' : y[40],
        'contact_company' : y[41],
        'contact_title' : y[42],
        'months_worked' : y[43],
        'rhs_company_sponsored' : y[44],
        'gs_admissions_status' : y[45],
        'deposit_fee_transaction_date' : y[46],
        'status' : y[47],
        'timestamp' : y[48]
    }

    print(student_view_sql_api_map)
    print(50 * '-')

    gradterm = student_view_sql_api_map['graduation_term'].split(' ')[0]
    if student_view_sql_api_map['graduation_term'].split(' ')[0] == 'Spring':
        gradclass = int(student_view_sql_api_map['graduation_term'].split(' ')[1])
    else:
        gradclass = int(student_view_sql_api_map['graduation_term'].split(' ')[1]) + 1
    
    if student_view_sql_api_map['status'] == 'Insert':

        # Hard coding visibility value only if status is 'Insert' as SF doesnt have this field and it is mandatory in HS
        visibility_value_final = 2
        # if y[8] is not None:
        #     visibility_value_final = int(contact_visibility_df.loc[contact_visibility_df['visibility_value']
        #                                                            == str(y[8]), 'visibility_id'].iloc[0])
        final_student_post = copy.deepcopy(default_df_student)

        final_student_post['CustomAttributeValues'][custom_attr_map['contact_id']] = student_view_sql_api_map['sf_contact_id']
        final_student_post['StudentID'] = student_view_sql_api_map['uid']
        final_student_post['FirstName'] = student_view_sql_api_map['first_name']
        final_student_post['MiddleName'] = student_view_sql_api_map['middle_name']
        final_student_post['LastName'] = student_view_sql_api_map['last_name']
        final_student_post['EmailAddress'] = student_view_sql_api_map['email']

        final_student_post['StudentGroups'] = []
        list_of_groups = []
        # To extract hs_program names
        # hs_program_name = program_df[program_df['sf_name'] == student_view_sql_api_map['program']]['hs_name'].values[0]
        
        if(student_view_sql_api_map['program'] == 'Undergrad BS'):
            list_of_groups.append('UG - New students')
        else:
            # process data based on presence or absence of Program_format field
            if student_view_sql_api_map['program_type'] == None:
                list_of_groups = program_mapping.loc[(program_mapping.Program_Degree_Name == student_view_sql_api_map['program']),'StudentGroups'].iloc[0].split(",")
                N_sem = int(program_mapping.loc[(program_mapping.Program_Degree_Name == student_view_sql_api_map['program']),'No. of semesters'].iloc[0])
            else: 
                list_of_groups = program_mapping.loc[(program_mapping.Program_Format == student_view_sql_api_map['program_type']) & (program_mapping.Program_Degree_Name == student_view_sql_api_map['program']),'StudentGroups'].iloc[0].split(",")
                N_sem = int(program_mapping.loc[(program_mapping.Program_Format == student_view_sql_api_map['program_type']) & (program_mapping.Program_Degree_Name == student_view_sql_api_map['program']),'No. of semesters'].iloc[0])
                
        for y,z in enumerate(list_of_groups):
            temp_dict = {}
            temp_dict['Name'] = z.strip()
            temp_dict['Id'] = int(student_group_df[student_group_df['hs_name'] == temp_dict['Name']]['hs_id'].values[0])
            final_student_post['StudentGroups'].append(temp_dict)

        final_student_post['Program']['Id'] = program_df[program_df['sf_name'] == student_view_sql_api_map['program']]['hs_id'].values[0]
        final_student_post['CustomAttributeValues'][custom_attr_map['start_term']] = gradterm_df[gradterm_df['sf_name'] == student_view_sql_api_map['start_term']]['hs_id'].values[0]
        final_student_post['GraduationTerm'] = gradterm
        final_student_post['GraduationClass'] = gradclass
        
        final_student_post['VisibilityId'] = visibility_value_final

        # # format datetime values to json format
        # if student_view_sql_api_map['create_date'] is not None: final_contacts_post['CreateDate'] = final_contacts_post['CreateDate'].strftime(
        #     '%Y-%m-%dT%H:%M:%S.%f%z')
        # if contact_sql_api_map['modify_date'] is not None: final_contacts_post['ModifyDate'] = final_contacts_post[
        #     'ModifyDate'].strftime(
        #     '%Y-%m-%dT%H:%M:%S.%f%z')

        # API Data Insertion
        PostDataUrl = 'https://' + url + '12twenty.com/api/v2/students'
        r = requests.post(PostDataUrl, json=final_student_post, headers=authHeader)

        contact_dict_values = [final_student_post['CustomAttributeValues'][custom_attr_map['contact_id']],
                               final_student_post['StudentID'], 
                               final_student_post['FirstName'],
                               final_student_post['MiddleName'],
                               final_student_post['LastName'],
                               final_student_post['EmailAddress'], 
                               final_student_post['StudentGroups'],
                               final_student_post['Program'],
                               final_student_post['CustomAttributeValues'][custom_attr_map['start_term']], 
                               final_student_post['GraduationTerm'],
                               final_student_post['GraduationClass'] 
                               ]
        vals_contact = None
        vals_contact = contact_dict_values + ['Insert'] + ['Hiresmith']  # contact_values to be updated

        print(vals_contact)

mycursor.execute(
    "INSERT INTO  " + mySchema + ".job_log (job_name, source, category, status) VALUES ('HS_Student_Step_2', "
                                    "'Hiresmith', 'End', 'Success');")
mydb.commit()

print("Job 2 ran successfully.")

        # if r.status_code != 201 and r.status_code != 200:
        #     vals_contact = vals_contact + [str(r.status_code)] + [r.text[0:9995]]
        #     mycursor.execute(script_student_error_log, vals_contact)
        #     mydb.commit()
