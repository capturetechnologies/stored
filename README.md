# FoundationDB layer
In development, not ready for use

## Connect to DB
```
db := stored.Connect("./fdb.cluster", "DB")
```

## Init object
```
dbUser = db.Object("user", User{}) // User could be struct in your project
```

## Set primary key
```
dbUser.Primary("id")
```
