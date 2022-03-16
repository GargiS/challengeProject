mvn clean install -DskipTests
mvn package

java -jar target/anz-code-challenge-UBER.jar \
--schema "/Users/gargi/Documents/InterviewQuestions/challengeProject/src/main/resources/scenarios/aus-capitals.json" \
--data "/Users/gargi/Documents/InterviewQuestions/challengeProject/src/main/resources/scenarios/aus-capitals.csv" \
--tag "/Users/gargi/Documents/InterviewQuestions/challengeProject/src/main/resources/scenarios/aus-capitals-invalid-1.tag"