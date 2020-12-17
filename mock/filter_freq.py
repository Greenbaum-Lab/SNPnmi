# deprecated - this can be done inline using vcftools
import pandas as pd
path = 'chr9_analysis.frq'
min_chr = 1671
df = pd.read_csv(path, sep='\t', names = ['CHROM','POS','N_ALLELES','N_CHR','F1','F2','F3','F4','F5','F6'])
# drop header
df = df[df['CHROM']!='CHROM']
# bi-allelic only
df_bi = df[df['N_ALLELES']=='2']
df_bi = df_bi[['CHROM','POS','N_ALLELES','N_CHR','F1','F2']]
df_bi[F_1] = df_bi['F1'].apply(lambda c_f: float(c_f.split(':')[1]))
df_bi['F_1'] = df_bi['F1'].apply(lambda c_f: float(c_f.split(':')[1]))
df_bi['F_2'] = df_bi['F2'].apply(lambda c_f: float(c_f.split(':')[1]))
df_bi['LOW_F'] = df_bi[['F_1','F_2']].min(axis=1)

# only if we have 90% valid entries
df_bi['N_CHR'] = df_bi['N_CHR'].astype(int)
max = df_bi['N_CHR']

df_bi[['CHROM', 'POS', 'N_CHR', 'LOW_F']]
# print history - useful!
# import readline; print('\n'.join([str(readline.get_history_item(i + 1)) for i in range(readline.get_current_history_length())]))
