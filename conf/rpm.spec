Name:      @name@
Version:   @version@
Release:   @release@
Packager:  Engineering Tools
Vendor:    Netflix
License:   NFLX
Summary:   Cerberus
Group:     NFLX/Application
BuildArch: @buildarch@
Requires:  zeromq jzmq storm
AutoReqProv: no

%description
Real Time Stream Processing System
----------
@build.metadata@

%install

# Degugging RPM spec variables vvv
echo "CWD(install): `pwd`"
echo "[[ _topdir:%{_topdir} ]]"
echo "[[ RPM_BUILD_DIR:$RPM_BUILD_DIR ]]"
echo "[[ RPM_BUILD_ROOT:$RPM_BUILD_ROOT ]]"
echo "[[ buildroot:%{buildroot} ]]"
echo "[[ summary:%{summary} name:%{name} version:%{version} release:%{release} packager:%{packager} ]]"
echo "[[ vendor:%{vendor} license:%{license} group:%{group} source0:%{source0} ]]"
echo ""
# Degugging RPM spec variables ^^^

%define appHome         apps/tomcat/webapps
%define tomcatBin       apps/tomcat/bin
%define tomcatConf      apps/tomcat/conf

mkdir -p $RPM_BUILD_ROOT/%{appHome}
mkdir -p $RPM_BUILD_ROOT/%{tomcatBin}
mkdir -p $RPM_BUILD_ROOT/%{tomcatConf}
cp $RPM_BUILD_DIR/%{name}.war $RPM_BUILD_ROOT/%{appHome}/ROOT.war
cp $RPM_BUILD_DIR/setenv.sh $RPM_BUILD_ROOT/%{tomcatBin}/.
cp $RPM_BUILD_DIR/server.xml $RPM_BUILD_ROOT/%{tomcatConf}/.
( cd $RPM_BUILD_DIR; rsync -avR apps etc ${RPM_BUILD_ROOT} )

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
/apps
/etc

%post
#
# Set the default system java to sunjdk7
#
/usr/bin/system-jdk --set sunjdk7
