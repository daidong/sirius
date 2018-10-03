CUR_DIR:=$(shell pwd)
THRIFT:=$(shell which thrift)
THRIFT_VER:=$(shell thrift -version)

JAVA:=$(shell which java)
JAVA_VER:=$(shell java -version 2>&1)

MAVEN:=$(shell which mvn)
MAVEN_VER:=$(shell mvn -version)

JAVA_SRC_HOME=$(CUR_DIR)/java

THRFT_SCHM=$(CUR_DIR)/thrift
THRFT_JAVA_NS=$(JAVA_SRC_HOME)/src/main/java
THRFT_JAVA_PATH=$(THRFT_JAVA_NS)/edu/dair/sgdb/thrift

.PHONY: print_info
print_info:
		# Here is the info corresponding to the essentials for building your system.
		# CUR_DIR	=	$(CUR_DIR);
		# THRIFT	=	$(THRIFT);
		# THRIFT_VER	=	$(THRIFT_VER);
		# JAVA		=	$(JAVA);
		# JAVA_VER	=	$(JAVA_VER);
		# MAVEN		=	$(MAVEN);
		# MAVEN_VER	=	$(MAVEN_VER);

.PHONY: clean
clean:
		cd $(JAVA_SRC_HOME) && mvn clean;
		-rm -rf release
		-rm -rf logs

clean-thrift:
		-rm -f $(THRFT_JAVA_PATH)/*.java

.PHONY: deepclean
deepclean: clean clean-thrift
		# Both thrift generated files and build files are cleaned!

thrift-java:
		$(THRIFT) --gen $(subst thrift-,,$@) -out $(THRFT_$(shell echo $(subst thrift-,,$@) | tr '[a-z]' '[A-Z]')_NS) $(THRFT_SCHM)/Commands.thrift;


.PHONY: thrift
thrift: thrift-java
		# Build thrift files.

.PHONY: install
install:
		# Build CPP lib
		# cd $(CPP_SRC_HOME) && make all;
		# Build Java Projects
		cd $(JAVA_SRC_HOME) && mvn clean install;
		mkdir -p release; mv java/target/*.tar.gz release/; cd release && tar zxf sgdb-0.1-make-assembly.tar.gz;

#cd release && tar zxf main.tar.gz

.PHONY: all
all: install
		# Start building
