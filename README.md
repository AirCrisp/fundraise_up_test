# FundraisUp Backend Test Task

## Prerequisites
To succesfully launch all applications you need MongoDB ReplicaSet in your environment.

## Install
```
git clone git@github.com:AirCrisp/fundraise_up_test.git
cd fundraise_up_test
npm ci
npm run build
cp .env.example .env
// set DB connection URI in .env file
```

## Usage
You can launch applications directly from build directory:
```
node build/app.js && build/sync.js && node build/sync.js --full-reindex
```

or you can use npm scripts:
```
npm run start:app
npm run start:sync
npm run start:reindex
```
