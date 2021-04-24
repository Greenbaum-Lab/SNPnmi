# TODO - verify the nubmer and size of files we have is the same as this in the source.
# Best if they have a checksum.

'''
The following was used adhoc to compare the sized of source to those on the cluster, after I pulled them manually.
Need to write a func to do that.

sizes_cluster = "C:\Data\HUJI\hgdp\sizes_cluster.tsv"
sizes_source = "C:\Data\HUJI\hgdp\sizes_source.tsv"
import pandas as pd
source = pd.read_csv(sizes_source, sep='\t', header=None, names =  ['key','size2'])
source['size2'] = source['size2'].str.replace(' KB', '')
source.head()
cluster = pd.read_csv(sizes_cluster, sep=' ', header=None, names =  ['size1','1','2','3','4','key'])
cluster['size1'] = cluster['size1'].str.replace('K', '')

cluster.head()
joined = cluster.join(source.set_index('key'), on='key')
joined = joined[joined['key'].str.endswith('.gz')]
joined[joined['size1']!=joined['size2']]

'''