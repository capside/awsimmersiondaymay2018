import sys
from awsglue import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import job
from pyspark.sql import SparkSession

glueContext = GlueContext(SparkContext.getOrCreate())   
medicare_dynamicframe = glueContext.create_dynamic_frame.from_catalog( database = "medicare", table_name = "medicare")

medicare_res = medicare_dynamicframe.resolveChoice(specs = [('provider id','cast:long')])

medicare_dataframe = medicare_res.toDF()
medicare_dataframe = medicare_dataframe.where("`provider id` is NOT NULL")


medicare_tmp_dyf = DynamicFrame.fromDF(medicare_dataframe, glueContext, 'DFname')
medicare_dyf = medicare_tmp_dyf.apply_mapping([
                 ('drg definition', 'string', 'drg', 'string'),
                 ('provider id', 'long', 'provider_id', 'long'),
                 ('provider name', 'string', 'provider_name', 'string'),
                 ('provider city', 'string', 'provider_city', 'string'),
                 ('provider state', 'string', 'provider_state', 'string'),
                 ('provider zip code', 'long', 'provider_zip', 'long'),
                 ('hospital referral region description', 'string','hospital_ref', 'string'),
                 ('average covered charges', 'string', 'charges_covered', 'double'),
                 ('average total payments', 'string', 'charges_total_pay', 'double'),
                 ('average medicare payments', 'string', 'charges_medicare_pay', 'double')
                 ])

glueContext.write_dynamic_frame.from_options(
       frame = medicare_dyf,
       connection_type = 's3',
       connection_options = {'path': 's3://crawler-test-mario/medicare_parquet'},
       format = 'parquet')
