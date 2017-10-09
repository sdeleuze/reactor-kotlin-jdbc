# Reactor Kotlin JDBC

Fluent, concise, and easy-to-use extension functions targeting JDBC in the Kotlin language with [Reactor Core](http://projectreactor.io/).

This library is a fork of Thomas Nield [rxkotlin-jdbc](https://github.com/thomasnield/rxkotlin-jdbc) which is inspired by Dave Moten's [RxJava-JDBC](https://github.com/davidmoten/rxjava-jdbc) but seeks to be much more lightweight by leveraging Kotlin functions. This works with threadpool `DataSource` implementations such as [HikariCP](https://github.com/brettwooldridge/HikariCP), but can also be used with vanilla JDBC `Connection`s.

Extension functions like `select()`, `insert()`, and `execute()` will target both `DataSource` and JDBC `Connection` types.

## DataSource Usage Examples

When you use a `DataSource`, a `Connection` will automatically be pulled from the pool upon subscription and given back when `onComplete` is called.


```kotlin
val config = HikariConfig()
config.jdbcUrl = "jdbc:sqlite::memory:"
config.minimumIdle = 3
config.maximumPoolSize = 10

val ds = HikariDataSource(config)

//initialize

with(ds) {
    execute("CREATE TABLE USER (ID INTEGER PRIMARY KEY, USERNAME VARCHAR(30) NOT NULL, PASSWORD VARCHAR(30) NOT NULL)")
    execute("INSERT INTO USER (USERNAME,PASSWORD) VALUES (?,?)", "thomasnield", "password123")
    execute("INSERT INTO USER (USERNAME,PASSWORD) VALUES (?,?)", "bobmarshal","batman43")
}

// Retrieve all users
ds.select("SELECT * FROM USER")
        .toFlux { it.getInt("ID") to it.getString("USERNAME") }
        .subscribe(::println)


// Retrieve user with specific ID
ds.select("SELECT * FROM USER WHERE ID = :id")
        .parameter("id", 2)
        .toMono { it.getInt("ID") to it.getString("USERNAME") }
        .subscribeBy(::println)

// Execute insert which return generated keys, and re-select the inserted record with that key
ds.insert("INSERT INTO USER (USERNAME, PASSWORD) VALUES (:username,:password)")
        .parameter("username","josephmarlon")
        .parameter("password","coffeesnob43")
        .toFlux { it.getInt(1) }
        .flatMap {
            conn.select("SELECT * FROM USER WHERE ID = :id")
                    .parameter("id", it)
                    .toMono { "${it.getInt("ID")} ${it.getString("USERNAME")} ${it.getString("PASSWORD")}" }
        }
        .subscribe(::println)

// Run deletion

conn.execute("DELETE FROM USER WHERE ID = :id")
        .parameter("id",2)
        .toMono()
        .subscribeBy(::println)
```

## Connection Usage Example

You can also use a standard `Connection` with these extension functions, and closing will not happen automatically so you can micromanage the life of that connection.

```kotlin
val connection = DriverManager.getConnection("jdbc:sqlite::memory:")

connection.select("SELECT * FROM USER")
        .toFlux { it.getInt("ID") to it.getString("USERNAME") }
        .subscribe(::println)

```


## Future Developments

* [ ] Batch write support
* [ ] Flux parameter inputs
