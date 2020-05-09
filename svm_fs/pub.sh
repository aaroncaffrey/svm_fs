rm -rf ~/svm_fs/svm_fs/pbs_ctl_sub
rm -rf ~/svm_fs/svm_fs/pbs_wkr_sub
rm -rf ~/svm_fs/svm_fs/pbs_ldr_sub
rm -rf ~/svm_fs/svm_fs/results
rm -rf ~/svm_fs/svm_fs/obj
rm -rf ~/svm_fs/svm_fs/bin

rm -rf /mmfs1/data/scratch/k1040015/svm_fs/pbs_ctl_sub
rm -rf /mmfs1/data/scratch/k1040015/svm_fs/pbs_wkr_sub
rm -rf /mmfs1/data/scratch/k1040015/svm_fs/pbs_ldr_sub
rm -rf /mmfs1/data/scratch/k1040015/svm_fs/results
rm -rf /mmfs1/data/scratch/k1040015/svm_fs/obj
rm -rf /mmfs1/data/scratch/k1040015/svm_fs/bin

git pull

mkdir /mmfs1/data/scratch/k1040015/svm_fs/pbs_ldr_sub
mkdir /mmfs1/data/scratch/k1040015/svm_fs/pbs_ctl_sub
mkdir /mmfs1/data/scratch/k1040015/svm_fs/pbs_wkr_sub

~/.dotnet/dotnet publish --self-contained -r linux-x64 -c Release
cd ~/svm_fs/svm_fs/bin/Release/netcoreapp3.1/linux-x64/publish/

# nohup ~/svm_fs/svm_fs/bin/Release/netcoreapp3.1/linux-x64/publish/svm_fs -cm ldr 1> /mmfs1/data/scratch/k1040015/svm_fs/pbs_ldr_sub/svm_ldr.stdout 2> /mmfs1/data/scratch/k1040015/svm_fs/pbs_ldr_sub/svm_ldr.stderr &
~/svm_fs/svm_fs/bin/Release/netcoreapp3.1/linux-x64/publish/svm_fs -cm ldr 1> /mmfs1/data/scratch/k1040015/svm_fs/pbs_ldr_sub/svm_ldr.stdout 2> /mmfs1/data/scratch/k1040015/svm_fs/pbs_ldr_sub/svm_ldr.stderr


