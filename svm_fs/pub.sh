rm -rf ~/svm_fs/svm_fs/pbs_ctl_sub
rm -rf ~/svm_fs/svm_fs/pbs_wkr_sub
rm -rf ~/svm_fs/svm_fs/pbs_ldr_sub
rm -rf ~/svm_fs/svm_fs/results
rm -rf ~/svm_fs/svm_fs/obj
rm -rf ~/svm_fs/svm_fs/bin
~/.dotnet/dotnet publish --self-contained -r linux-x64 -c Release
cd ~/svm_fs/svm_fs/bin/Release/netcoreapp3.1/linux-x64/publish/
