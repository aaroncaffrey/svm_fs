rm -rf pbs_sub
rm -rf results
rm -rf obj
rm -rf bin
~/.dotnet/dotnet publish --self-contained -r linux-x64 -c Release
cp ./svm_fs.runtimeconfig.json ./bin/Release/netcoreapp3.0/linux-x64/publish/
cd ./bin/Release/netcoreapp3.0/linux-x64/publish/


