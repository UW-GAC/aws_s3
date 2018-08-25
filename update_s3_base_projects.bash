#!/bin/bash
# sync the base /projects/topmed folders to s3
# base folders:
#   /projects/topmed/downloaded_data/IRC_freezes/freeze.5b/gds/minDP0/*chr*
#   /projects/topmed/analyses/hematology/analysts/zhengx/fr5b/HCT/relatedness/data
#   /projects/topmed/analyses/hematology/analysts/zhengx/fr5b/HCT/results/*.RData
#   /projects/topmed/analyses/hematology/analysts/zhengx/fr5b/HGB/relatedness/data
#   /projects/topmed/analyses/hematology/analysts/zhengx/fr5b/HGB/results/*.RData
#   /projects/topmed/analysts//sdmorris/freeze5/memory_usage
#   /projects/topmed/analysts//sdmorris/freeze5/compute_test

f () {
    errcode=$? # save the exit code as the first thing done in the trap function
    echo "error $errorcode"
    echo "the command executing at the time of the error was"
    echo "$BASH_COMMAND"
    echo "on line ${BASH_LINENO[0]}"
    # do some error handling, cleanup, logging, notification
    # $BASH_COMMAND contains the command that was being executed at the time of the trap
    # ${BASH_LINENO[0]} contains the line number in the script of that command
    # exit the script or return to try again, etc.
    exit $errcode  # or use some other value or do return instead
}
trap f ERR

python2.7 ~/bin/update_projects.py -s /projects/topmed/downloaded_data/IRC_freezes/freeze.5b/gds/minDP0/ -i "*chr*" -r -N -l freeze5b.log

python2.7 ~/bin/update_projects.py -s /projects/topmed/analyses/hematology/analysts/zhengx/fr5b/HCT/relatedness/data/HCT_grm.gds -N -l zheng_hct_relatedness.log

python2.7 ~/bin/update_projects.py -s /projects/topmed/analyses/hematology/analysts/zhengx/fr5b/HCT/results/HCT_allChrom_f5_mutiEthn_CondnL.RData -N -l zheng_hct_results1.log

python2.7 ~/bin/update_projects.py -s /projects/topmed/analyses/hematology/analysts/zhengx/fr5b/HCT/results/HCT_allChrom_f5_mutiEthn_CondnL_reduced_set.RData -N -l zheng_hct_results2.log

python2.7 ~/bin/update_projects.py -s /projects/topmed/analyses/hematology/analysts/zhengx/fr5b/HCT/results/HCT_sample_v1.RData -N -l zheng_hct_results3.log

python2.7 ~/bin/update_projects.py -s /projects/topmed/analyses/hematology/analysts/zhengx/fr5b/HCT/results/HCT_trait_full_v1.RData -N -l zheng_hct_results4.log

python2.7 ~/bin/update_projects.py -s /projects/topmed/analyses/hematology/analysts/zhengx/fr5b/HCT/results/HCT_trait_v1.RData -l zheng_hct_results5.log

python2.7 ~/bin/update_projects.py -s /projects/topmed/analyses/hematology/analysts/zhengx/fr5b/HGB/relatedness/data  -r -N -l zheng_hgb_relatedness.log

python2.7 ~/bin/update_projects.py -s /projects/topmed/analyses/hematology/analysts/zhengx/fr5b/HGB/results/HGB_allChrom_f5_mutiEthn_CondnL.RData -N -l zheng_hgb_results1.log

python2.7 ~/bin/update_projects.py -s /projects/topmed/analyses/hematology/analysts/zhengx/fr5b/HGB/results/HGB_allChrom_f5_mutiEthn_CondnL_reduced_set.RData -N -l zheng_hgb_results2.log

python2.7 ~/bin/update_projects.py -s /projects/topmed/analyses/hematology/analysts/zhengx/fr5b/HGB/results/HGB_sample_v1.RData -N -l zheng_hgb_results3.log

python2.7 ~/bin/update_projects.py -s /projects/topmed/analyses/hematology/analysts/zhengx/fr5b/HGB/results/HGB_trait_full_v1.RData -N -l zheng_hgb_results4.log

python2.7 ~/bin/update_projects.py -s /projects/topmed/analyses/hematology/analysts/zhengx/fr5b/HGB/results/HGB_trait_v1.RData -N -l zheng_hgb_results5.log

python2.7 ~/bin/update_projects.py -s /projects/topmed/analysts/sdmorris/freeze5/memory_usage -N -r -l mem_usage.log

# last one; send message
python2.7 ~/bin/update_projects.py -s /projects/topmed/analysts/sdmorris/freeze5/compute_test -r -l compute_test.log
