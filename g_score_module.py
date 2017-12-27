#!usr/bin/python3
from cleaning import build_name_list
import pandas as pd
import numpy as np


class YearData(object):
	"""Current year data + reference to previous shit"""
	def __init__(self):
		'''Returns pandas file of all year shit'''
		year_list_decile = {}
		year_list = {}
		name_list = build_name_list(2011,6)
		for i in range(len(name_list)):
			year_list_decile[name_list[i]] = pd.read_csv('cleaned_data/clean_sorted/' + name_list[i] + '_clean_sorted.csv')
		name_list = build_name_list(2007,10)
		for i in range(len(name_list)):
			year_list[name_list[i]] = pd.read_csv('data_collected/data/' + name_list[i] + '_yearly.csv')
		

		self.year_list_decile = year_list_decile
		self.year_list = year_list
		
		self.g_score_data_20 = {}
		self.g_score_data = {}
		self.g_scores = {}
		

	def initialise_g_score(self, data):
		'''Initialises all parameters'''
		if data ==0:
			name_list = build_name_list(2011,6)
			for i in range(len(name_list)):
				df = pd.DataFrame()
				df['nic'] = self.year_list_decile[name_list[i]]['nic']
				df['sa_finance1_cocode'] = self.year_list_decile[name_list[i]]['sa_finance1_cocode']
				df['sa_company_name'] = self.year_list_decile[name_list[i]]['sa_company_name']
				self.g_score_data_20[name_list[i]] = df
				
			name_list = build_name_list(2007,10)
			for i in range(len(name_list)):
				df = pd.DataFrame()
				df['sa_finance1_cocode'] = self.year_list[name_list[i]]['sa_finance1_cocode']
				df['sa_company_name'] = self.year_list[name_list[i]]['sa_company_name']
				self.g_score_data[name_list[i]] = df

		elif data ==1:
			name_list = build_name_list(2011,6)
			for i in range(len(name_list)):
				link_des = 'g_score_data_20/'+name_list[i]+'_g_score_data_20.csv'
				g_score_data_20 = pd.read_csv(link_des, header=None)
				self.g_score_data_20[name_list[i]] = g_score_data_20
				
			name_list = build_name_list(2007,10)
			for i in range(len(name_list)):
				link_des = 'g_score_data/'+name_list[i]+'_g_score_data.csv'
				g_score_data = pd.read_csv(link_des, header=None)
				self.g_score_data[name_list[i]] = g_score_data
			
	def average_assets_all(self):
		'''Get average assets'''
		name_list = build_name_list(2007,10)
		for i in range(len(name_list)-1):
			average_assets_yearly = []
			for company in self.year_list[name_list[i+1]]['sa_company_name']:
				df = self.year_list[name_list[i+1]]
				assets_t1 = df[df['sa_company_name'].str.match(company)]['sa_total_assets']
				df = self.year_list[name_list[i]]
				assets_t0 = df[df['sa_company_name'].str.match(company)]['sa_total_assets']
				try:
					average_assets = (float(assets_t0.values[0]))
					average_assets_yearly.append(average_assets)
				except:
					try:
						average_assets = (float(assets_t1.values[0]))
						average_assets_yearly.append(average_assets)
					except:
						average_assets_yearly.append(float('NaN'))
			average_assets_yearly = pd.DataFrame(average_assets_yearly)
			self.g_score_data[name_list[i+1]] = pd.concat([self.g_score_data[name_list[i+1]], average_assets_yearly], axis = 1, ignore_index=True)

		name_list = build_name_list(2011,6)
		for i in range(len(name_list)):
			average_assets_yearly_20 = []
			#link_des = 'g_score_data/'+name_list[i]+'g_score_data.csv'
			#g_score_data = pd.read_csv(link_des, header=None)
			g_score_data = self.g_score_data[name_list[i]]
			for company_20 in self.year_list_decile[name_list[i]]['sa_finance1_cocode']:
				crap = []
				crap.append(str(company_20))
				df = g_score_data[g_score_data[0].isin(crap)][2]
				average_assets_20 = float(df.values[0])
				average_assets_yearly_20.append(average_assets_20)
			average_assets_yearly_20 = pd.DataFrame(average_assets_yearly_20)
			self.g_score_data_20[name_list[i]] = pd.concat([self.g_score_data_20[name_list[i]], average_assets_yearly_20], axis = 1, ignore_index=True)	


	def get_g_score_data(self, i):
		name_list = build_name_list(2007,10)
		print(name_list[i])
		print(self.g_score_data[name_list[i]])

	def get_g_score_data_20(self, i):
		name_list = build_name_list(2011,6)
		print(name_list[i])
		print(self.g_score_data_20[name_list[i]])

	def get_g_scores(self, i):
		name_list = build_name_list(2011,6)
		print(name_list[i])
		print(self.g_scores[name_list[i]])

	def g_score_data_to_csv(self):
		name_list = build_name_list(2007,10)
		for i in range(len(name_list)):
			self.g_score_data[name_list[i]].to_csv('g_score_data/' + name_list[i] + '_g_score_data.csv', header=False, index=False)

	def g_score_data_20_to_csv(self):
		name_list = build_name_list(2011,6)
		for i in range(len(name_list)):
			self.g_score_data_20[name_list[i]].to_csv('g_score_data_20/' + name_list[i] + '_g_score_data_20.csv', header=False, index=False)

	def g_scores_to_csv(self):
		name_list = build_name_list(2011,6)
		for i in range(len(name_list)):
			self.g_scores[name_list[i]].to_csv('g_scores/' + name_list[i] + '_g_scores.csv', header=False, index=False)


	def roa(self):
		name_list = build_name_list(2008,9)
		for i in range(len(name_list)):
			roa_yearly = []
			year_list = self.year_list[name_list[i]]
			g_score_data = self.g_score_data[name_list[i]]
			for company in self.year_list[name_list[i]]['sa_company_name']:
				df1 = year_list[year_list['sa_company_name'].str.match(company)].fillna(0)
				df2 = g_score_data[g_score_data[1].str.match(company)][2]
				try:
					roa = (float(df1['sa_pat'].values[0])+float(df1['sa_extra_ordi_inc'].values[0]))/float(df2.values[0])
					roa_yearly.append(roa)
				except:
					roa_yearly.append(float('NaN'))

			roa_yearly = pd.DataFrame(roa_yearly)
			self.g_score_data[name_list[i]] = pd.concat([self.g_score_data[name_list[i]], roa_yearly], axis = 1, ignore_index=True)
		name_list = build_name_list(2011,6)
		for i in range(len(name_list)):
			roa_yearly_20 = []
			#link_des = 'g_score_data/'+name_list[i]+'g_score_data.csv'
			#g_score_data = pd.read_csv(link_des, header=None)
			g_score_data = self.g_score_data[name_list[i]]
			for company_20 in self.year_list_decile[name_list[i]]['sa_finance1_cocode']:
				crap = []
				crap.append(str(company_20))
				df = g_score_data[g_score_data[0].isin(crap)][3]
				roa_20 = float(df.values[0])
				roa_yearly_20.append(roa_20)
			roa_yearly_20 = pd.DataFrame(roa_yearly_20)
			self.g_score_data_20[name_list[i]] = pd.concat([self.g_score_data_20[name_list[i]], roa_yearly_20], axis = 1, ignore_index=True)

	def cfo(self):
		name_list = build_name_list(2011,6)
		for i in range(len(name_list)):
			cfo_yearly = []
			year_list_decile = self.year_list_decile[name_list[i]]
			g_score_data_20 = self.g_score_data_20[name_list[i]]
			for company in g_score_data_20.iloc[:,2]:
				df1 = year_list_decile[year_list_decile['sa_company_name'].str.match(company)]['sa_cf_net_frm_op_activity']
				df2 = g_score_data_20[g_score_data_20[2].str.match(company)][3]
				try:
					cfo = float(df1.values[0])/float(df2.values[0])
					cfo_yearly.append(cfo)
				except:
					cfo_yearly.append(float('NaN'))
			cfo_yearly = pd.DataFrame(cfo_yearly)
			self.g_score_data_20[name_list[i]] = pd.concat([self.g_score_data_20[name_list[i]], cfo_yearly], axis = 1, ignore_index=True)
		
	def accruals(self):
		name_list = build_name_list(2011,6)
		for i in range(len(name_list)):
			accruals_yearly = []
			g_score_data_20 = self.g_score_data_20[name_list[i]]
			for company in g_score_data_20.iloc[:,2]:
				df = g_score_data_20[g_score_data_20[2].str.match(company)]
				try:
					cfo = df[5].values[0]
					roa = df[4].values[0]
					accruals = cfo - roa
					accruals_yearly.append(accruals)
				except:
					accruals_yearly.append(float('NaN'))
			accruals_yearly = pd.DataFrame(accruals_yearly)
			self.g_score_data_20[name_list[i]] = pd.concat([self.g_score_data_20[name_list[i]], accruals_yearly], axis = 1, ignore_index=True)

	def roa_var(self):
		name_list = build_name_list(2008,9)

		for i in range(len(name_list)-3):
			roa_var_yearly = []
			for company in self.year_list_decile[name_list[i+3]]['sa_company_name']:
				roa_past = []
				for j in range(3):
					g_score_data = self.g_score_data[name_list[i+j]]
					df = g_score_data[g_score_data[1].str.match(company)][3]	
					try:
						roa_past.append(df.values[0])
					except:
						pass
				try:
					roa_past = pd.DataFrame(roa_past)
					if roa_past.shape[0] == 0 or roa_past.shape[0] == 1:
						roa_var = float('inf')
					else:
						roa_var = (roa_past.var()).values[0]
					roa_var_yearly.append(roa_var)
				except:
					roa_var_yearly.append(float('inf'))
			roa_var_yearly = pd.DataFrame(roa_var_yearly)
			self.g_score_data_20[name_list[i+3]] = pd.concat([self.g_score_data_20[name_list[i+3]], roa_var_yearly], axis = 1, ignore_index=True)

	def sales_var(self):
		name_list = build_name_list(2007,10)
		for i in range(len(name_list)-4):
			sales_var_yearly = []
			for company in self.g_score_data_20[name_list[i+4]].iloc[:,2]:
				sales_past = []
				for j in range(3):
					sales_1 = self.year_list[name_list[i+j]]
					df1 = sales_1[sales_1['sa_company_name'].str.match(company)]['sa_sales'].fillna(0)
					sales_2 = self.year_list[name_list[i+j+1]]
					df2 = sales_2[sales_2['sa_company_name'].str.match(company)]['sa_sales'].fillna(0)
					try:
						a = (df2.values[0] - df1.values[0])
						sales_past.append(a)
					except:
						pass
				try:
					sales_past = pd.DataFrame(sales_past)
					if sales_past.shape[0] == 0 or sales_past.shape[0] == 1:
						sales_var = float(float('inf'))
					else:
						sales_var = (sales_past.var()).values[0]
					sales_var_yearly.append(sales_var)
				except:
					sales_var_yearly.append(float('inf'))
			sales_var_yearly = (pd.DataFrame(sales_var_yearly))
			self.g_score_data_20[name_list[i+4]] = pd.concat([self.g_score_data_20[name_list[i+4]], sales_var_yearly], axis = 1, ignore_index=True)

	def rnd(self):
		name_list = build_name_list(2011,6)
		for i in range(len(name_list)):
			rnd_yearly = []
			year_list_decile = self.year_list_decile[name_list[i]]
			g_score_data_20 = self.g_score_data_20[name_list[i]]
			for company in g_score_data_20.iloc[:,2]:
				df1 = year_list_decile[year_list_decile['sa_company_name'].str.match(company)]['sa_rnd_exp'].fillna(0)
				df2 = g_score_data_20[g_score_data_20[2].str.match(company)][3]
				try:
					rnd = float(df1.values[0])/float(df2.values[0])
					rnd_yearly.append(rnd)
				except:
					rnd_yearly.append(float('NaN'))
			rnd_yearly = pd.DataFrame(rnd_yearly)
			self.g_score_data_20[name_list[i]] = pd.concat([self.g_score_data_20[name_list[i]], rnd_yearly], axis = 1, ignore_index=True)

	def advert(self):
		name_list = build_name_list(2011,6)
		for i in range(len(name_list)):
			advert_yearly = []
			year_list_decile = self.year_list_decile[name_list[i]]
			g_score_data_20 = self.g_score_data_20[name_list[i]]
			for company in g_score_data_20.iloc[:,2]:
				df1 = year_list_decile[year_list_decile['sa_company_name'].str.match(company)]['sa_advertising'].fillna(0)
				df2 = g_score_data_20[g_score_data_20[2].str.match(company)][3]
				try:
					advert = float(df1.values[0])/float(df2.values[0])
					advert_yearly.append(advert)
				except:
					advert_yearly.append(float('NaN'))
			advert_yearly = pd.DataFrame(advert_yearly)
			self.g_score_data_20[name_list[i]] = pd.concat([self.g_score_data_20[name_list[i]], advert_yearly], axis = 1, ignore_index=True)

	def capex(self):
		name_list = build_name_list(2011,6)
		for i in range(len(name_list)):
			capex_yearly = []
			year_list_decile = self.year_list_decile[name_list[i]].fillna(0)
			g_score_data_20 = self.g_score_data_20[name_list[i]]
			for company in g_score_data_20.iloc[:,2]:
				df1 = year_list_decile[year_list_decile['sa_company_name'].str.match(company)]
				df2 = g_score_data_20[g_score_data_20[2].str.match(company)][3]
				try:
					capex = float(df1['sa_building_net_addn_in_yr'].values[0]) + float(df1['sa_comm_equip_net_addn_in_yr'].values[0]) + float(df1['sa_computer_it_net_addn_in_yr'].values[0]) + float(df1['sa_elec_install_fitting_net_addn_in_yr'].values[0]) + float(df1['sa_gfa_net_addn_in_yr'].values[0]) + float(df1['sa_net_furn_social_oth_fxd_ast'].values[0]) + float(df1['sa_plant_net_addn_in_yr'].values[0]) + float(df1['sa_sw_net_addn_in_yr'].values[0]) + float(df1['sa_transport_infra_net_addn_in_yr'].values[0]) + float(df1['sa_transport_veh_net_addn_in_yr'].values[0])
					capex = capex/float(df2.values[0])
					capex_yearly.append(capex)
				except:
					capex_yearly.append(float('NaN'))
			capex_yearly = pd.DataFrame(capex_yearly)
			self.g_score_data_20[name_list[i]] = pd.concat([self.g_score_data_20[name_list[i]], capex_yearly], axis = 1, ignore_index=True)

	def g_score_calc(self):
		g_scores = {}
		name_list = build_name_list(2011,6)
		for z in range(len(name_list)):
			g_score = self.g_score_data_20[name_list[z]].fillna(float('inf'))
			for j in range(g_score.shape[1]-4):
				ref = 0
				ref_index = -1
				first = 0
				g_score_int = g_score.reset_index(drop=True)
				n = g_score.shape[0]
				for index, nic in g_score.iloc[:, 0].iteritems():
					if ref != int(nic) or index == n-1:
						inf_count = 0
						if first == 0:
							g = g_score.iloc[ref_index:index,:]
							first = first+1
							g = sort(g, j+4, 1)
							n = g.shape[0]
							for i in range(n):
								if j == 2:
									if g.iloc[i,j+4] > 0:
										g.iloc[i,j+4] = 1
									else:
										g.iloc[i,j+4] = 0
								else:	
									if g.iloc[i,j+4] == float('inf'):
										g.iloc[i,j+4] = 0
										inf_count = inf_count + 1
									else:
										if i<(n-inf_count)/2:
											g.iloc[i, j+4] = 1
										else:
											g.iloc[i,j+4] = 0
							g_score_int = g
						elif first==1:
							g = g_score.iloc[ref_index:index,:]
							g = sort(g, j+4, 1)
							n = g.shape[0]
							for i in range(n):
								if g.iloc[i,j+4] == float('inf'):
									g.iloc[i,j+4] = 0
									inf_count = inf_count + 1
								else:
									if i<(n-inf_count)/2:
										g.iloc[i, j+4] = 1
									else:
										g.iloc[i,j+4] = 0
							g_score_int = pd.concat([g_score_int, g], axis = 0, ignore_index=True)
						ref = int(nic)
						ref_index = index
					else:
						pass
				g_score = g_score_int
			g_1 = []
			for i in range(g_score.shape[0]):	
				a = 0
				for j in range(7):
					a = a + g_score.iloc[i,j+4]
				g_1.append(a)
			g_score = pd.concat([g_score, pd.DataFrame(g_1)], axis = 1, ignore_index=True)
			g_scores = sort(g_score, 12, 1).reset_index(drop=True)
			g_scores_1 = g_scores.iloc[:, 0:3]
			g_scores_2 = g_scores.iloc[:,12]
			self.g_scores[name_list[z]] = pd.concat([g_scores_1, g_scores_2], axis = 1, ignore_index=True)





#cleaning_data_all()
#a = YearData()
#a.initialise_g_score(1)
#a.average_assets_all()
#a.roa()
#a.cfo()
#a.accruals()
#a.roa_var()
#a.sales_var()
#a.rnd()
#a.advert()
#a.capex()
#a.g_score_calc()
#a.g_score_data_to_csv()
#a.g_score_data_20_to_csv()
#a.g_scores_to_csv()
#a.get_g_score_data(4)
#a.get_g_score_data_20(0)



