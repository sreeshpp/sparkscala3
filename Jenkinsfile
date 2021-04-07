#!/usr/bin/env groovy

node{
    stage('Prepare')   {
        cleanWs()
        git(
        url: 'https://github.com/sreeshpp/sparkscala3.git',
        credentialsId: 'sreeshpp',
        branch: 'master3'
        )
    }
    stage('Build'){
        if(isUnix()){
        echo 'unix os'
        sh 'pwd'
        sh 'chmod +x gradlew'
        sh './gradlew clean build -xtest --info'
  }
  else{
    echo 'not a unix os'
  }
    }
}
