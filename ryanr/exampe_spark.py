import logging
from data_transform.flatfile import DirectMailConstants
from data_transform.flatfile.dm_file_loader import DMFileLoader

####### Note : this is just reference code copied from my previous spark jobs ############
​
dm_config = DirectMailConstants()
​
def run_job(spark, **job_args):
    target_mailing_fact_table = job_args.get('target_table_mailing', 'f_prospect_mailing')
    target_prospect_lkup_table = job_args.get('target_table_prospects', 'lkup_prospect')
    campaigns_to_load = sorted(list(dm_config.campaign_cutoff_dates.keys()))
    camps = iter(campaigns_to_load)
    camp1 = next(camps)
    logging.info(f'generating {camp1}')
    first_cutoff_date = dm_config.campaign_cutoff_dates.get(camp1)
    first_loader = DMFileLoader(spark, camp1, first_cutoff_date, target_prospect_lkup_table, target_mailing_fact_table)
    first_loader.process_file(overwrite=True)

    for camp in camps:
        logging.info(f'generating {camp}')
        campaign_start_date = dm_config.campaign_cutoff_dates.get(camp)
        loader = DMFileLoader(spark, camp, campaign_start_date, target_prospect_lkup_table, target_mailing_fact_table)
        loader.process_file(overwrite=False)
        logging.info(f'generated {camp}!')

    def recast_df_columns(base_dataframe, nonstring_types_dict={}):
        """
        prevent null type in dataframe for columns that are null -- force recast everything to a string
        except those in the types dict, which has a form like
               {'prospect_id': T.LongType(),
              'record_nb': T.IntegerType()
                }
        :param base_dataframe:
        :param nonstring_types_dict:
        :return:
        """
        return base_dataframe.select(*(col(c).cast(nonstring_types_dict.get(c, StringType())).alias(c)
                                       for c in base_dataframe.columns))