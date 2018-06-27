pipeline {
    agent any

    environment {
        ORG = 'sds'
        PROJECT = 'iomgr'
        CONAN_CHANNEL = 'testing'
        CONAN_USER = 'sds'
        CONAN_PASS = credentials('CONAN_PASS')
    }

    stages {
        stage('Build') {
            steps {
                sh "docker build --build-arg CONAN_USER=${CONAN_USER} --build-arg CONAN_PASS=${CONAN_PASS} --build-arg CONAN_CHANNEL=${CONAN_CHANNEL} -t ${PROJECT} ."
            }
        }

        stage('Test') {
            steps {
                echo "Tests go here"
            }
        }

        stage('Deploy') {
            when {
                branch 'stable/*'
            }
            steps {
                sh "docker run ${PROJECT}"
            }
        }
    }
}
