pipeline {
    agent any

    environment {
        PATH = "C:\\Users\\ahmad\\AppData\\Local\\Programs\\Python\\Python313\\Scripts;C:\\Users\\ahmad\\AppData\\Local\\Programs\\Python\\Python313;${env.PATH}"
    }

    stages {
        stage('Check Environment') {
            steps {
                bat 'where py'
                bat 'where pipenv'
            }
        }

        stage('Build') {
            steps {
                bat '''
                echo 🛠️ Checking for Python launcher...
                echo 🛠️ Checking if pipenv is available in PATH...
                echo ✅ All requirements found. Initializing pipenv with Python 3.13...
                pipenv --python 3.13
                pipenv sync
                '''
            }
        }

        stage('Test') {
            steps {
                bat '''
                echo ▶ Running tests...
                pipenv run pytest
                '''
            }
        }

        // ❌ Removed Package stage here

        stage('Release') {
            when {
                expression { currentBuild.resultIsBetterOrEqualTo('SUCCESS') }
            }
            steps {
                echo '🚀 Release stage (placeholder)'
            }
        }

        stage('Deploy') {
            when {
                expression { currentBuild.resultIsBetterOrEqualTo('SUCCESS') }
            }
            steps {
                echo '📦 Deploy stage (placeholder)'
            }
        }
    }
}

