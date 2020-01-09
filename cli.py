################################################
#     Module title: cli.py
#    Author: JP Aldama
#    GitHub: https://www/github.com/sysad-aldama
#    Date: 1/3/2020
################################################
#
#    [FUNCTIONS]:
#        process(in_file:<str>, num_receipts:<int>, out_file:<str>)
#            concat_receipts()
#            collapse_data()
#            process_best_sellers()
#            write_output()
#    [USAGE]:
#        cli.py -i <in_file> -n <num_receipts> -o <out_file>
#    [NOTE]:
#        Please do not use file extensions.
#            -i use data/ or current working directory
#            -n (+)ive, non zero, non (-)ive integers only
#            -o Enter the filename without it's extension. Default is 'out_file.json'
#    [REQ]:
#        External Modules Used: 
#            pandas 0.25.1, click 7.0
# #################################################

from glob import glob
import pandas as pd
import sys
import os
import time
import json
import click
from datetime import datetime


@click.command()
@click.option("--in", "-i", "in_file", required=True,  # default='data/',
              help="Path to receipts to be processed. Use data/ or current working directory",
              )
@click.option("--number", "-n", "num_receipts", required=True,  # default=10,
              help="Number of receipts to process. Accepts (+)ive, non zero, non (-)ive integers only"
              )
@click.option("--out-file", "-o", "out_file", required=True,  # default="out_file",
              help="Path to output file to store the result. Enter the filename without it's extension."
              )
def process(in_file, num_receipts, out_file):
    """ [FUNCTIONS]:
            process(in_file:<str>, num_receipts:<int>, out_file:<str>)
                concat_receipts()
                collapse_data()
                write_output()\n

        [DESCRIPTION]:
            The receipts path IN, amount of best selling products/rank and stores the result to
            output file OUT.
    """

    stop = int(num_receipts)
    output_list = []
    globbed = in_file+'*.json'
    out_file_ = out_file+'.json'
    start_time = time.time()
    now = datetime.now()
    run_date = now
    print('_____________________Processing_________________________')

    def concat_receipts():
        """ Take receipts and merge N num_receipts into one file for processing """
        count = 1
        for f in glob(globbed):
            while stop >= count:
                with open(f, "rb") as infile:
                    output_list.append(json.load(infile))
                    count += 1
                    break
        with open(out_file_, 'w') as d:
            json.dump(output_list, d, indent=4)

    def collapse_data():
        """ Collapse and extract relevant data from N num_receipts to prepare for analysys """
        with open(out_file_, 'r') as f:
            read_ = json.load(f)
            __products__ = []
            _products_ = {}
            for output__ in read_:
                products__ = output__['products']
                for product__items in products__:
                    _products_ = {
                        'product_id': product__items['product_id'], 'qty_sold': product__items['qty_sold']}
                    __products__.append(_products_)
            with open(out_file_, 'w') as write_out_file:
                json.dump(__products__, write_out_file, indent=4)

    def process_best_sellers():
        """ Perform ETL operation and out put into the format according to specifications """
        df = pd.read_json(out_file_)
        df['product__id'] = df['product_id']
        df.groupby(['product__id', 'qty_sold']).count(
        ).sort_values('qty_sold', ascending=False)
        df['rank'] = df['qty_sold'].rank(ascending=False)
        df_sorted = df.sort_values('rank')
        new_df = df_sorted.drop('product__id', axis=1)
        new_df1 = new_df
        new_df1 = pd.DataFrame(new_df1)
        new_df1['rank'] = new_df1['rank'].astype(int)
        new_df1['qty_sold'] = new_df1['qty_sold'].astype(float)
        new_df1 = new_df1.reset_index(drop=True)
        new_df1.to_json(out_file_, orient='index')

    def write_output():
        """ Take processed data, construct best sellers JSON format save to JSON file """
        with open(out_file_, 'r') as datafile:
            data = json.load(datafile)
        data = list(data.values())
        data_list = data
        data_list = {"source_folder": os.getcwd(),
                     "run_date": run_date.strftime('%Y-%m-%d'),
                     "file_count": stop,
                     "best_sellers": data[0:stop]
                     }
        with open(out_file_, 'w') as fp:
            data_list = json.dump(data_list, fp, indent=4)

    concat_receipts()
    collapse_data()
    process_best_sellers()
    write_output()

    print('_______________________Done!___________________________')
    print('________Running time: %s seconds________: ' %
          (time.time() - start_time))


if __name__ == "__main__":
    process()
