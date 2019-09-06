import os
import sys
import pandas as pd
import boto3
import psycopg2
import yaml
import click


def parse_yaml_from_s3(bucket, prefix):
    s3 = boto3.resource('s3')
    obj = s3.Bucket(bucket).Object(prefix).get()['Body'].read()
    return yaml.load(obj)


def get_current_iteration(params):
    incoming_s3_path = os.path.join("s3://" + params['learner']['bucket'],
                                    params['learner']['prefix'], params['learner']['incoming_names'])
    incoming_names = pd.read_csv(incoming_s3_path)
    current_iteration = max(list(incoming_names['iteration'][incoming_names['run'] == params['learner']['runid']])) + 1
    return int(current_iteration)


def insert_metrics(params):
    """Read the current iteration metrics from incoming_metrics csv on s3 
    and insert into table (therefore var is "outgoing").
    This step must occur before insert_names due to validity constraints"""

    outgoing_metrics_path = os.path.join("s3://" + params['learner']['bucket'],
                                         params['learner']['prefix'], params['learner']['metrics'])
    outgoing_metrics = pd.read_csv(outgoing_metrics_path)
    # Connect
    con = psycopg2.connect(host=params["labeller"]["db_host"], database=params["labeller"]["db_production_name"],
                           user=params["labeller"]["db_username"], password=params["labeller"]["db_password"])
    curs = con.cursor()
    print('cursor made')

    # Update the iteration_metrics table
    try:
        insert_query = "insert into iteration_metrics " \
                       "(run, iteration, tss, accuracy, aoi, iteration_time, precision, " \
                       "recall, fpr, tpr, auc) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s); "
        outgoing_metrics = outgoing_metrics[outgoing_metrics['iteration'] == get_current_iteration(params)]
        outgoing_metrics = outgoing_metrics[outgoing_metrics['run'] == params['learner']['runid']]
        # this is needed for multiple runs for multiple aois. incoming_names.csv will need an aoi column and its
        # corresponding table will need to have a aoi column that is a key like run and iteration
        # or we have a different incoming_names.csv for each aoi
        # outgoing_metrics = outgoing_metrics[outgoing_metrics['run']==params['learner']['aoiid']]
        outgoing_metrics = outgoing_metrics.reindex(
            columns=["run", "iteration", "tss", "accuracy", "aoi", "iteration_time", "precision", "recall", "fpr",
                     "tpr", "AUC"])
        outgoing_list = list(outgoing_metrics.iloc[0])
        # converts numpy types to basic python types for DB
        for i, n in enumerate(outgoing_list):
            if type(n) is not str:
                outgoing_list[i] = n.item()
        curs.execute(insert_query, outgoing_list)
        con.commit()
        print('Finished saving out the iteration metrics')
    except psycopg2.DatabaseError as err:
        print("Error updating database")
        print(err)
    finally:
        if con:
            con.close()


def insert_names(params):
    """Put top uncertain grid names in incoming_names table.
    from https://github.com/agroimpacts/mapperAL/blob/lsong/common/dummy_cvml.py"""

    outgoing_names_df = pd.read_csv(params['learner']['outgoing'])
    if len(outgoing_names_df) > 0:
        con = psycopg2.connect(host=params["labeller"]["db_host"], database=params["labeller"]["db_production_name"],
                               user=params["labeller"]["db_username"], password=params["labeller"]["db_password"])
        print('done connecting')
        curs = con.cursor()

        # Update the incoming_names table
        try:
            outgoing_names_df = outgoing_names_df[outgoing_names_df['iteration'] == get_current_iteration(params)]
            outgoing_names_df = outgoing_names_df[outgoing_names_df['run'] == params['learner']['runid']]
            for index, row in outgoing_names_df.iterrows():
                row = list(row)
                insert_query = "insert into incoming_names (name, run, iteration, processed, usage) " \
                               "values (%s, %s, %s, %s, %s) "
                curs.execute(insert_query, row)
            con.commit()
            print('Finished saving out the incoming_names')
        except psycopg2.DatabaseError as err:
            print("Error updating database")
            print(err)
        finally:
            if con:
                con.close()
    else:
        print('Need to download new images...')
        sys.exit(2)


@click.command()
@click.option('--config-filename', default='cvmapper_config.yaml', help='The name of the config to use.')
def main(config_filename):
    params = parse_yaml_from_s3("activemapper", config_filename)
    insert_metrics(params)
    insert_names(params)


if __name__ == "__main__":
    main()
