from pyspark.sql import *
from pyspark.sql.functions import *
import os
import sys
import traceback

class USACrashCaseStudy:
	def __init__(self, spark):
		"""Reading all dataframes"""
		self.df_charges_use = spark.read.format("csv").options(header ="True", inferSchema = "True").load("Charges_use.csv")
		self.df_damages_use = spark.read.format("csv").options(header ="True", inferSchema = "True").load("Damages_use.csv")
		self.df_endorse_use = spark.read.format("csv").options(header ="True", inferSchema = "True").load("Endorse_use.csv")
		self.df_primary_person_use = spark.read.format("csv").options(header ="True", inferSchema = "True").load("Primary_Person_use.csv")
		self.df_restrict_use = spark.read.format("csv").options(header ="True", inferSchema = "True").load("Restrict_use.csv")
		self.df_units_use = spark.read.format("csv").options(header ="True", inferSchema = "True").load("Units_use.csv")
	
	def solution_1(self, spark):
		"""To calculate Number of crashes in which number of persons killed who are male"""
		killed_male = self.df_primary_person_use.where((col("PRSN_INJRY_SEV_ID") == lit("KILLED")) & (col("PRSN_GNDR_ID") == lit("MALE"))).distinct().count()
		print("")
		print("Solution 1:")
		print("Number of crashes in which number of persons killed who are male : "+ str(killed_male))
		
	def solution_2(self, spark):
		"""To find two wheelers booked for crashes"""
		two_wheeler_crash = self.df_primary_person_use.where((col("PRSN_TYPE_ID") == lit("DRIVER OF MOTORCYCLE TYPE VEHICLE"))).distinct().count()
		print("")
		print("Solution 2:")
		print("Two wheelers booked for crashes : "+ str(two_wheeler_crash))
	
	def solution_3(self, spark):
		"""To calculate state having maximum crashes involving female"""
		df_gender = self.df_primary_person_use.where(col("PRSN_GNDR_ID") == lit("FEMALE")).select(col("CRASH_ID"), col("UNIT_NBR")).distinct()
		
		df_state = self.df_units_use.select(col("CRASH_ID"), col("UNIT_NBR"), col("VEH_LIC_STATE_ID")).distinct()
		
		state_max_female_crash = (df_gender.join(df_state,["CRASH_ID", "UNIT_NBR"])
				.groupBy(col("VEH_LIC_STATE_ID")).agg(count(lit("*")).alias("crash_count")))
						  
		spec = Window.orderBy(col("crash_count").desc())
		max_crash_state = state_max_female_crash.withColumn("Rank", dense_rank().over(spec)).filter(col("Rank") == lit(1)).select(col("VEH_LIC_STATE_ID")).collect()[0][0]
		print("")
		print("Solution 3:")
		print("State having maximum crashes involving Female : "+ max_crash_state)
		
	def solution_4(self, spark):
		"""To calculate top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death are"""
		df_injury = (self.df_primary_person_use.where(~(col("PRSN_INJRY_SEV_ID").isin(lit("NA"),lit("UNKNOWN"),lit("NOT INJURED"))))
				.select(col("CRASH_ID"), col("UNIT_NBR"))).distinct()
		
		df_vehicle = (self.df_units_use
				.where(~(col("VEH_MAKE_ID").isin(lit("NA"), lit("UNKNOWN"))))
				.select(col("CRASH_ID"), col("UNIT_NBR"), col("VEH_MAKE_ID"))).distinct()

		df_most_injury = (df_injury.join(df_vehicle,["CRASH_ID", "UNIT_NBR"])
				.groupBy(col("VEH_MAKE_ID")).agg(count(lit("*")).alias("VEH_COUNT"))
				)
				 
		win_crash = Window.orderBy(col("VEH_COUNT").desc())

		most_crash_car = (df_most_injury.withColumn("RANK", rank().over(win_crash))
				.where(col("RANK").between(lit(5), lit(15)))
				.select(col("VEH_MAKE_ID"))
				.collect()
				)
		print("")
		print("Solution 4:")
		print("Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death are : ",", ".join([x[0] for x in most_crash_car]))
	
	def solution_5(self, spark):
		"""To calculate top ethnic user group of each vehicle unique body style involved in crash"""
		df_ethnicity = (self.df_primary_person_use
				.where(~(col("PRSN_ETHNICITY_ID").isin(lit("NA"), lit("UNKNOWN"))))
				.select(col("CRASH_ID"), col("UNIT_NBR"), col("PRSN_ETHNICITY_ID")).distinct())

		df_veh_body_type = (self.df_units_use
				.where(~(col("VEH_BODY_STYL_ID").isin(lit("NA"),lit("NOT REPORTED"),lit("UNKNOWN"))))
				.select(col("CRASH_ID"), col("UNIT_NBR"), col("VEH_BODY_STYL_ID")).distinct()
				)

		df_join = (df_ethnicity.join(df_veh_body_type, ["CRASH_ID", "UNIT_NBR"])
				.groupBy(col("VEH_BODY_STYL_ID"), col("PRSN_ETHNICITY_ID")).agg(count(lit("*")).alias("VEH_BODY_ETHNIC_COUNT"))
				)

		win_veh_body_ethnic = Window.partitionBy(col("VEH_BODY_STYL_ID")).orderBy(col("VEH_BODY_ETHNIC_COUNT").desc())

		veh_body_ethnic_count_df = (df_join.withColumn("RANK", dense_rank().over(win_veh_body_ethnic))
				.where(col("RANK") == lit(1)).select(col("VEH_BODY_STYL_ID"), col("PRSN_ETHNICITY_ID"))
				)
		print("")
		print("Solution 5:")
		print("Top ethnic user group of each vehicle unique body style involved in crash:")
		print(dict([(x[0],x[1]) for x in veh_body_ethnic_count_df.collect()]))
		
	def solution_6(self, spark):
		"""To Calculate top 5 Zip code with highest number crashes with alcohols as the contributing factor"""
		df_contri_factor = (self.df_units_use
				.where((col("CONTRIB_FACTR_1_ID").like("%ALCOHOL%")) | (col("CONTRIB_FACTR_1_ID").like("%DRINKING%"))
				|
				(col("CONTRIB_FACTR_2_ID").like("%ALCOHOL%")) | (col("CONTRIB_FACTR_2_ID").like("%DRINKING%"))
				|
				(col("CONTRIB_FACTR_P1_ID").like("%ALCOHOL%")) | (col("CONTRIB_FACTR_P1_ID").like("%DRINKING%"))
				)
				.select(col("CRASH_ID"), col("UNIT_NBR"))
				.distinct()
				)

		df_zip_code = (self.df_primary_person_use.where(col("DRVR_ZIP") != lit("UNKNOWN"))
				.select(col("CRASH_ID"), col("UNIT_NBR"), col("DRVR_ZIP"))
				.distinct()
				)

		df_zipcode_alcohol = (df_zip_code.join(df_contri_factor,["CRASH_ID", "UNIT_NBR"])
				.groupBy(col("DRVR_ZIP")).agg(count(lit("*")).alias("ZIP_CODE_COUNT"))
                )
	
		win_zipcode = Window.orderBy(col("ZIP_CODE_COUNT").desc())

		max_zip_code = (df_zipcode_alcohol.withColumn("RANK",rank().over(win_zipcode))
				.where(col("RANK") < lit(6))
				.select(col("DRVR_ZIP"))
                ).collect()
		print("")
		print("Solution 6:")
		print("Top 5 Zip code with highest number crashes with alcohols as the contributing factor: ", ", ".join([x[0] for x in max_zip_code]) )
		
		
	def solution_7(self, spark):
		"""To Calculate count of Distinct Crash IDs where No Damaged Property was observed and Damage Level is above 4 and car avails Insurance"""
		df_damage_insurance = (self.df_units_use
				.where(~(col("FIN_RESP_TYPE_ID").isin(lit("NA")))
				& 
				((col("VEH_DMAG_SCL_1_ID").isin(lit("DAMAGED 5"), lit("DAMAGED 6"), lit("DAMAGED 7 HIGHEST"))) 
				| 
				(col("VEH_DMAG_SCL_2_ID").isin(lit("DAMAGED 5"), lit("DAMAGED 6"), lit("DAMAGED 7 HIGHEST"))))
				)
				.select(col("CRASH_ID"))
				.distinct()
				)

		df_no_damage = (self.df_damages_use
				.where((col("DAMAGED_PROPERTY").like("%NO DAMAGE%")) | (col("DAMAGED_PROPERTY").like("%NO DMG%")))
				.select(col("CRASH_ID"))
				.distinct()
				)

		crash_no_damage = (df_damage_insurance.join(df_no_damage,["CRASH_ID"]).count())
		print("")
		print("Solution 7:")
		print("Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level is above 4 and car avails Insurance is : "+ str(crash_no_damage))

	def solution_8(self, spark):
		"""Top 5 Vehicle Makes with multiple conditions"""
		#extracting top 10 vehicle color
		df_color = (self.df_units_use
				.where(~(col("VEH_COLOR_ID").isin(lit("98"), lit("99"), lit("NA"))))
				.groupBy(col("VEH_COLOR_ID")).agg(count(lit("*")).alias("COLOR_COUNT"))
				)
			
		win = Window.orderBy(col("COLOR_COUNT").desc())
		top_10_color = (df_color.withColumn("RANK",rank().over(win)).where(col("RANK")<lit(11))
				.select(col("VEH_COLOR_ID"))
				).collect()

		top_10_color_list = [x[0] for x in top_10_color]
		
		#Extracting top 25 states
		df_state_count = (self.df_units_use.join(self.df_charges_use,["CRASH_ID", "UNIT_NBR"])
				.select(col("CRASH_ID"),col("UNIT_NBR"),col("VEH_LIC_STATE_ID")).distinct()
				.groupBy(col("VEH_LIC_STATE_ID")).agg(count(lit("*")).alias("STATE_COUNT"))
                )

		win_states = Window.orderBy(col("STATE_COUNT").desc())

		high_offence_state = (df_state_count.withColumn("RANK",rank().over(win_states))
				.where(col("RANK")<26)
				.select(col("VEH_LIC_STATE_ID"))
				).collect()
				
		high_offence_state_list = [x[0] for x in high_offence_state]
		
		#######################################
		
		df_unit_info = (self.df_units_use
				.where(((col("CONTRIB_FACTR_1_ID").like("%SPEEDING%")) | 
                (col("CONTRIB_FACTR_2_ID").like("%SPEEDING%")) | 
                (col("CONTRIB_FACTR_P1_ID").like("%SPEEDING%")))
                &
                (col("VEH_COLOR_ID").isin(top_10_color_list))
                &
                (col("VEH_LIC_STATE_ID").isin(high_offence_state_list))
                )
                .select(col("CRASH_ID"), col("UNIT_NBR"), col("VEH_MAKE_ID"))                 
				)
               
		df_lic_driver = (self.df_primary_person_use
                .where(~(col("DRVR_LIC_TYPE_ID").isin(lit("NA"), lit("UNKNOWN"), lit("UNLICENSED"))))
                .select(col("CRASH_ID"), col("UNIT_NBR"))
                )
		
		df_top_5_veh_makes = (df_lic_driver.join(df_unit_info,["CRASH_ID", "UNIT_NBR"]).distinct()
                .groupBy(col("VEH_MAKE_ID")).agg(count(lit("*")).alias("VEH_MAKE_ID_COUNT"))
                )

		#calculating top 5 vehicle make id
		win_make_id_count = Window.orderBy(col("VEH_MAKE_ID_COUNT").desc())

		top_5_veh_makes = (df_top_5_veh_makes.withColumn("RANK",rank().over(win_make_id_count)).where(col("RANK")<6)
				.select(col("VEH_MAKE_ID"))
				.collect()
				)

		top_5_veh_makes_list = [x[0] for x in top_5_veh_makes]
		print("")
		print("Solution 8:")
		print("Top 5 Vehicle Makes : "+ ", ".join(top_5_veh_makes_list))
	

if __name__ == "__main__":
        spark = SparkSession.builder.appName("USACrashCaseStudy").getOrCreate()
        
        obj = USACrashCaseStudy(spark)
        try:             
            obj.solution_1(spark)
            obj.solution_2(spark)
            obj.solution_3(spark)
            obj.solution_4(spark)
            obj.solution_5(spark)
            obj.solution_6(spark)
            obj.solution_7(spark)
            obj.solution_8(spark)
            
            spark.stop()
            
        except Exception as e:
            print("Process failed due to : "+ str(e))
            sys.exit(1)
            

