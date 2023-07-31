Hi Reviewer,

I completed the Stedi Project as per the given project instructions and rubrics. I get the help from Mentor and knowledge page for any doubts during my implementation of this project.

I included the SQL files, screenshots, python files for below zones

Landing Zone
=================
	Customer Landing Table
	----------------------
	Customer Landing Table is created based on the customer JSON file in the S3 folder. This data contains all the customer data irrespective of sharewithresearchasofdate is having value or not.
	customer_landing.sql is provided in repository along with screenshots
	
	accelerometer_landing Table
	-----------------------------
	Accelerometer Landing Table is created based on the multiple accelerometer JSON file in the S3 folder. This data contains all the accelerometer data irrespective of customer agreed for data sharing.
	
	step_trainer_landing Table
	-----------------------------
	Step Trainer Landing Table is created based on the multiple step trainer JSON file in the S3 folder. This data contains all the step trainer data irrespective of customer agreed for data sharing.
	
Trusted Zone
=============
	customer_trusted Table
	---------------------------
	This customer trusted table is loaded using the Glue Job(Python script): customer_landing_to_trusted.py, which filtered the data where customer agreed to share data for research (sharewithresearchasofdate is not null).
	
	accelerometer_trusted Table
	-------------------------------
	This accelerometer trusted table is loaded using the Glue Job(Python script): accelerometer_landing_to_trusted_zone.py, which joined accelerometer_landing with customer_trusted data on Customer.email = Accelerometer.user to ensure only accelerometer readings of the customer agreed to share is stored in accelerometer_trusted table.
	Note: Only Distinct email from customer_trusted table before join.

Curated Zone
=============
	customer_curated Table
	---------------------------
	This customer curated table is loaded using the Glue Job(Python script): Customer_trusted_to_curated.py, where customer data is loaded only if it has matching accelerometer record joined on basis of fields: Customer.email = Accelerometer.user.
	
	step_trainer_trusted Table
	--------------------------
	This step trainer trusted table is loaded using the Glue Job(Python script): trainer_landing_to_trusted.py, where step trainer data is loaded only if it has matching customer record joined on basis of fields: Customer.serialnumber = steptrainer.serialnumber
	Note: Only Distinct SerialNumber from customer_curated table before join.
	
	machine_learning_curated Table
	------------------------------
	This final aggregated table is created by joining the step_trainer_trusted tabel with accelerometer_trusted Table based on Accelerometer.Timestamp and StepTrainer.SensorReadingTime.
	Note: Email ID is removed before loading in the table.
	
	
Please check and let me know your suggestions.

Thanks in advance.