
#!/bin/bash
# The script is to be executed in the destination folder (not smart), something like: /vcf/hgdp/
# TODO - this list should be in the cofig, and we should read it from there.
# TODO - use the python script, it is nicer.. (might need some adjustmets)
for file in "README.data-access.hgdp_wgs.20190516.txt" "hgdp_wgs.20190516.full.chrY.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr21.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr22.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr19.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr20.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr18.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr16.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr17.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr15.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr14.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr13.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr9.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr12.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr10.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr11.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr8.vcf.gz.tbi" "hgdp_wgs.20190516.full.chrX.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr7.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr6.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr5.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr4.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr3.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr1.vcf.gz.tbi" "hgdp_wgs.20190516.full.chr2.vcf.gz.tbi" "hgdp_wgs.20190516.full.chrY.vcf.gz" "hgdp_wgs.20190516.full.chr21.vcf.gz" "hgdp_wgs.20190516.full.chr22.vcf.gz" "hgdp_wgs.20190516.full.chr20.vcf.gz" "hgdp_wgs.20190516.full.chr19.vcf.gz" "hgdp_wgs.20190516.full.chr18.vcf.gz" "hgdp_wgs.20190516.full.chr15.vcf.gz" "hgdp_wgs.20190516.full.chr17.vcf.gz" "hgdp_wgs.20190516.full.chrX.vcf.gz" "hgdp_wgs.20190516.full.chr14.vcf.gz" "hgdp_wgs.20190516.full.chr16.vcf.gz" "hgdp_wgs.20190516.full.chr13.vcf.gz" "hgdp_wgs.20190516.full.chr9.vcf.gz" "hgdp_wgs.20190516.full.chr12.vcf.gz" "hgdp_wgs.20190516.full.chr11.vcf.gz" "hgdp_wgs.20190516.full.chr10.vcf.gz" "hgdp_wgs.20190516.full.chr8.vcf.gz" "hgdp_wgs.20190516.full.chr7.vcf.gz" "hgdp_wgs.20190516.full.chr6.vcf.gz" "hgdp_wgs.20190516.full.chr5.vcf.gz" "hgdp_wgs.20190516.full.chr4.vcf.gz" "hgdp_wgs.20190516.full.chr3.vcf.gz" "hgdp_wgs.20190516.full.chr2.vcf.gz" "hgdp_wgs.20190516.full.chr1.vcf.gz"
do
	if [ -f ./$file ]; then
		echo "$file exists."
	else 
		wget ftp://ngs.sanger.ac.uk/production/hgdp/hgdp_wgs.20190516/$file
	fi
done