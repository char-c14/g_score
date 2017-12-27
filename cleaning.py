#!usr/bin/python3

import csv
import numpy as np
import pandas as pd
import os

def blank_remover(df_input, column_no):

	for index, row in df_input.iterrows():
		for i in column_no:
			df_input = df_input[np.isfinite(df_input[i])]
	return df_input

def build_name_list(a,b):
	
	name_list = []
	for i in range(b):
		name = str(i+a)
		name_list.append(name)
	return name_list

def tab_to_comma():
	
	name_list = build_name_list(2007,6)

	for i in range(4):

		filename = 'data_collected/' + name_list[i] + '_t.csv'
		output = 'data_collected/' + name_list[i] + '.csv'
		with open(filename, 'r') as read_file:
			with open(output, 'w') as write_file:
				data_reader = csv.reader(read_file, delimiter = "\t")
				data_writer = csv.writer(write_file, delimiter = ',')
				for row in data_reader:
					data_writer.writerow(row)


def sort(df_input, column, z):
	
	df = df_input.sort_values(by=column,ascending=False)
	n = (df.shape)[0]
	k = int(n/z + z)
	df = df.iloc[0:k,]
	return df

def merged_yearwise(filename):
	
	df = pd.read_csv(filename)
	name_list = build_name_list(2007,10)
	n = 2007
	for i in range(10):
		df_yearly = pd.DataFrame()
		for j in range(df.shape[0]):	
			if int(df['sa_finance1_year'][j]/10000) == (n+i):
				df_yearly = pd.concat([df_yearly, df.iloc[j,:]], axis = 1, ignore_index=True)
		df_yearly = df_yearly.transpose()
		df_yearly.to_csv('data_collected/' + filename + '/'+ name_list[i] + '_yearly.csv', index=False)

		
def delete_median(df_input):
	ref = 0
	ref_index = -1
	first = 0
	df_input = df_input.reset_index(drop=True)
	n = df_input.shape[0]
	for index, nic in df_input['nic'].iteritems():
		if ref != int(nic) or index == n-1:
			if (index - ref_index) > 2:
				if first == 0:
					df_output = df_input.iloc[ref_index:index,:]
					first = first+1
				elif first==1:
					df_output = pd.concat([df_output, df_input.iloc[ref_index:index,:]], axis = 0, ignore_index=True)
			else:
				pass
			ref = int(nic)
			ref_index = index
		else:
			pass

	return df_output

def data_merge(df, name):
	df_data = pd.read_csv('data_collected/data/' + name + '_yearly.csv')
	df_output = pd.DataFrame()
	for j in range(df.shape[0]):
		for i in range(df_data.shape[0]):
			if df_data['sa_finance1_cocode'][i] == df['co_code'][j]:
				df_output = pd.concat([df_output, df_data.iloc[i,:]], axis = 1, ignore_index=True)
				break
	return df_output.transpose()
	
def cleaning_data_all():
	
	name_list = build_name_list(2011,6)
	for i in range(len(name_list)):
		df_input_pb = pd.read_csv('data_collected/pb_files/' + name_list[i] + '.csv')
		df_input_pb = blank_remover(df_input_pb, ['nse_pb'])
		df_input_pb = sort(df_input_pb, 'nse_pb', 4.9)
		df_input_pb = df_input_pb.reset_index(drop=True)
		df_input_data = data_merge(df_input_pb, name_list[i])
		df_input_data = nic_merge(df_input_data)
		df_input_pb.to_csv('cleaned_data/clean/' + name_list[i] + '_pb' + '.csv', index=False)
		df_input = sort(df_input_data, ['nic'] , 1)
		df_input = blank_remover(df_input, ['nic'])
		df_input = delete_median(df_input)
		df_input.to_csv('cleaned_data/clean_sorted/' + name_list[i] + '_clean_sorted' + '.csv', index=False)
		print(name_list[i] + ' Done \n')


def nic_merge(df_input):
	nic_data = pd.read_csv('NIC.csv', header=None)
	nic_yearly = []
	for company in df_input['sa_company_name']:
		df = nic_data[nic_data[1].str.match(company)]
		try:
			nic_yearly.append((df[4].values)[0])
		except:
			nic_yearly.append(float('nan'))
	df_input['nic'] = nic_yearly
	return df_input

# cleaning_data_all()

def merged_yearwise_sales():
	
	name_list = build_name_list(2003,12)
	n = 2003
	for i in range(12):
		with open('sales/' + name_list[i] + '_yearly.csv', 'w') as write_file:
			with open('sales_data.csv',) as read_file:
				data_writer = csv.writer(write_file)
				data_reader = csv.reader(read_file)
				count = 0
				for row in data_reader:
					if count != 0:
						if row[2][0:4] == str(n+i):
							data_writer.writerow(row)
					count = count + 1

#merged_yearwise('data.csv')
#cleaning_data_all()