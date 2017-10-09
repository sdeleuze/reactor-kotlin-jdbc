package io.projectreactor.addons.kotlin.jdbc
import org.junit.Test
import reactor.core.publisher.Flux
import reactor.test.test
import java.sql.DriverManager

class JdbcExtensionsTest {

    private val dbPath = "jdbc:sqlite::memory:"

    private val connectionFactory = {

        DriverManager.getConnection(dbPath).apply {
            createStatement().apply {
                execute("CREATE TABLE USER (ID INTEGER PRIMARY KEY, USERNAME VARCHAR(30) NOT NULL, PASSWORD VARCHAR(30) NOT NULL)")
                execute("INSERT INTO USER (USERNAME,PASSWORD) VALUES ('thomasnield','password123')")
                execute("INSERT INTO USER (USERNAME,PASSWORD) VALUES ('bobmarshal','batman43')")
                close()
            }
        }
    }

    @Test
    fun testConnection() {

        val conn = connectionFactory()

        conn.select("SELECT * FROM USER")
                .toFlux { it.getInt("ID") to it.getString("USERNAME") }
                .test()
                .expectNextCount(2)
                .verifyComplete()

        conn.close()
    }

    @Test
    fun multiInjectParameterTest() {

        val conn = connectionFactory()

        conn.select("SELECT * FROM USER WHERE USERNAME LIKE :pattern and PASSWORD LIKE :pattern")
                .parameter("pattern","%b%")
                .toFlux { it.getInt("ID") to it.getString("USERNAME") }
                .test()
                .expectNext(Pair(2,"bobmarshal"))
                .verifyComplete()

        conn.close()
    }

    @Test
    fun parameterTest() {
        val conn = connectionFactory()

        conn.select("SELECT * FROM USER WHERE ID = ?")
                .parameter(2)
                .toMono { it.getInt("ID") to it.getString("USERNAME") }
                .test()
                .expectNext(Pair(2,"bobmarshal"))
                .verifyComplete()

        conn.close()
    }

    @Test
    fun namedParameterTest() {

        val conn = connectionFactory()

        conn.select("SELECT * FROM USER WHERE ID = :id")
                .parameter("id",2)
                .toMono { it.getInt("ID") to it.getString("USERNAME") }
                .test()
                .expectNext(Pair(2,"bobmarshal"))
                .verifyComplete()

        conn.close()
    }


    @Test
    fun namedMultipleParameterTest() {

        val conn = connectionFactory()

        conn.select("SELECT * FROM USER WHERE USERNAME LIKE :pattern and PASSWORD LIKE :pattern")
                .parameter("pattern","%b%")
                .toFlux { it.getInt("ID") to it.getString("USERNAME") }
                .test()
                .expectNext(Pair(2,"bobmarshal"))
                .verifyComplete()

        conn.close()
    }

    @Test
    fun flatMapSelectTest() {
        val conn = connectionFactory()

        Flux.just(1,2)
                .flatMap {
                    conn.select("SELECT * FROM USER WHERE ID = :id")
                            .parameter("id",it)
                            .toMono { it.getInt("ID") to it.getString("USERNAME") }
                }
                .test()
                .expectNextCount(2)
                .verifyComplete()

        conn.close()
    }

    @Test
    fun singleInsertTest() {
        val conn = connectionFactory()

        conn.insert("INSERT INTO USER (USERNAME, PASSWORD) VALUES (:username,:password)")
                .parameter("username","josephmarlon")
                .parameter("password","coffeesnob43")
                .toFlux { it.getInt(1) }
                .flatMap {
                    conn.select("SELECT * FROM USER WHERE ID = :id")
                            .parameter("id", it)
                            .toMono { "${it.getInt("ID")} ${it.getString("USERNAME")} ${it.getString("PASSWORD")}" }
                }
                .test()
                .expectNext("3 josephmarlon coffeesnob43")
                .verifyComplete()

        conn.close()
    }

    @Test
    fun multiInsertTest() {
        val conn = connectionFactory()

        Flux.just(
                Pair("josephmarlon", "coffeesnob43"),
                Pair("samuelfoley","shiner67"),
                Pair("emilyearly","rabbit99")
        ).flatMap {
            conn.insert("INSERT INTO USER (USERNAME, PASSWORD) VALUES (:username,:password)")
                    .parameter("username",it.first)
                    .parameter("password",it.second)
                    .toMono { it.getInt(1) }
        }.test()
                .expectNext(3,4,5)
                .verifyComplete()

        conn.close()
    }

    @Test
    fun deleteTest() {

        val conn = connectionFactory()

        conn.execute("DELETE FROM USER WHERE ID = :id")
                .parameter("id",2)
                .toMono()
                .test()
                .expectNext(1)
                .verifyComplete()

        conn.close()
    }
    @Test
    fun updateTest() {

        val conn = connectionFactory()

        conn.execute("UPDATE USER SET PASSWORD = :password WHERE ID = :id")
                .parameter("id",1)
                .parameter("password","squirrel56")
                .toMono()
                .test()
                .expectNext(1)
                .verifyComplete()

        conn.close()
    }
}