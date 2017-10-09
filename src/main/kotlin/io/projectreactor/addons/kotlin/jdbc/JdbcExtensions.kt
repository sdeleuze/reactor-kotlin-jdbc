package io.projectreactor.addons.kotlin.jdbc

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.toMono
import java.io.InputStream
import java.math.BigDecimal
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement.RETURN_GENERATED_KEYS
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

fun Connection.execute(sqlTemplate: String) = UpdateOperation(
		sqlTemplate = sqlTemplate,
		connectionGetter = { this },
		autoClose = false
)

fun Connection.select(sqlTemplate: String)  =
		SelectOperation(
				sqlTemplate = sqlTemplate,
				connectionGetter = { this },
				autoClose = false
		)

/**
 * Executes an INSERT operation and returns the generated keys as a single-field `ResultSet`
 */
fun Connection.insert(insertSQL: String)  =
		InsertOperation(
				sqlTemplate = insertSQL,
				connectionGetter = { this },
				autoClose = false
		)

fun DataSource.execute(sqlTemplate: String) = UpdateOperation(
		sqlTemplate = sqlTemplate,
		connectionGetter = { connection },
		autoClose = true
)

fun DataSource.select(sqlTemplate: String)  =
		SelectOperation(
				sqlTemplate = sqlTemplate,
				connectionGetter = { this.connection },
				autoClose = true
		)



fun DataSource.insert(insertSQL: String) =
		InsertOperation(
				sqlTemplate = insertSQL,
				connectionGetter = { this.connection },
				autoClose = true
		)


class PreparedStatementBuilder(
        val connectionGetter: () -> Connection,
        val preparedStatementGetter: (String,Connection) -> PreparedStatement,
        sqlTemplate: String

) {

    private val namelessParameterIndex = AtomicInteger(0)
    val sql: String = sqlTemplate.replace(parameterRegex,"?")
    val furtherOps: MutableList<(PreparedStatement) -> Unit> = mutableListOf()

    companion object {
        private val parameterRegex = Regex(":[_A-Za-z0-9]+")
    }

    private val mappedParameters = parameterRegex.findAll(sqlTemplate).asSequence()
            .map { it.value }
            .withIndex()
            .groupBy({it.value},{it.index})

    fun parameters(vararg parameters: Pair<String,Any?>) {
        parameters.forEach { parameter(it) }
    }

    fun parameter(value: Any?) {
        furtherOps += { it.processParameter(namelessParameterIndex.getAndIncrement(), value) }
    }

    fun parameters(vararg parameters: Any?) {
        furtherOps += { it.processParameters(parameters) }
    }
    fun parameter(parameter: Pair<String,Any?>) {
        parameter(parameter.first, parameter.second)
    }
    fun parameter(parameter: String, value: Any?) {
        (mappedParameters[":" + parameter] ?: throw Exception("Parameter $parameter not found!}"))
                .asSequence()
                .forEach { i -> furtherOps += { it.processParameter(i, value) } }
    }
    fun toPreparedStatement(): ConnectionAndPreparedStatement {
        val conn = connectionGetter()
        val ps = preparedStatementGetter(sql, conn)
        furtherOps.forEach { it(ps) }
        return ConnectionAndPreparedStatement(conn, ps)
    }
}

class ConnectionAndPreparedStatement(val conn: Connection, val ps: PreparedStatement)

class SelectOperation(
        sqlTemplate: String,
        connectionGetter: () -> Connection,
        val autoClose: Boolean
) {

    val builder = PreparedStatementBuilder(connectionGetter, { sql, conn -> conn.prepareStatement(sql) }, sqlTemplate)

    fun parameters(vararg parameters: Pair<String,Any?>): SelectOperation {
        builder.parameters(parameters)
        return this
    }

    fun parameter(value: Any?): SelectOperation {
        builder.parameter(value)
        return this
    }

    fun parameters(vararg parameters: Any?): SelectOperation {
        builder.parameters(parameters)
        return this
    }
    fun parameter(parameter: Pair<String,Any?>): SelectOperation {
        builder.parameter(parameter)
        return this
    }
    fun parameter(parameter: String, value: Any?): SelectOperation {
        builder.parameter(parameter,value)
        return this
    }

    fun <T: Any> toFlux(mapper: (ResultSet) -> T) = Flux.defer {
        val cps = builder.toPreparedStatement()
        ResultSetState({ cps.ps.executeQuery() }, cps.ps, cps.conn, autoClose).toFlux(mapper)
    }

    fun <T: Any> toMono(mapper: (ResultSet) -> T) = Mono.defer {
        val cps = builder.toPreparedStatement()
        ResultSetState({ cps.ps.executeQuery() }, cps.ps, cps.conn, autoClose).toMono(mapper)
    }

}

class InsertOperation(
        sqlTemplate: String,
        connectionGetter: () -> Connection,
        val autoClose: Boolean
) {

    val builder = PreparedStatementBuilder(connectionGetter, { sql, conn -> conn.prepareStatement(sql, RETURN_GENERATED_KEYS) }, sqlTemplate)

    fun parameters(vararg parameters: Pair<String,Any?>): InsertOperation {
        builder.parameters(parameters)
        return this
    }

    fun parameter(value: Any?): InsertOperation {
        builder.parameter(value)
        return this
    }

    fun parameters(vararg parameters: Any?): InsertOperation {
        builder.parameters(parameters)
        return this
    }
    fun parameter(parameter: Pair<String,Any?>): InsertOperation {
        builder.parameter(parameter)
        return this
    }
    fun parameter(parameter: String, value: Any?): InsertOperation {
        builder.parameter(parameter,value)
        return this
    }

    fun <T: Any> toFlux(mapper: (ResultSet) -> T) = Flux.defer {
        val cps = builder.toPreparedStatement()
        ResultSetState({
            cps.ps.executeUpdate()
            cps.ps.generatedKeys
        }, cps.ps, cps.conn, autoClose).toFlux(mapper)
    }

    fun <T: Any> toMono(mapper: (ResultSet) -> T) = Mono.defer {
        toFlux(mapper).single()
    }

}

class UpdateOperation(
        sqlTemplate: String,
        connectionGetter: () -> Connection,
        val autoClose: Boolean
) {

    val builder = PreparedStatementBuilder(connectionGetter, { sql, conn -> conn.prepareStatement(sql) }, sqlTemplate)

    fun parameters(vararg parameters: Pair<String,Any?>): UpdateOperation {
        builder.parameters(parameters)
        return this
    }

    fun parameter(value: Any?): UpdateOperation {
        builder.parameter(value)
        return this
    }

    fun parameters(vararg parameters: Any?): UpdateOperation {
        builder.parameters(parameters)
        return this
    }
    fun parameter(parameter: Pair<String,Any?>): UpdateOperation {
        builder.parameter(parameter)
        return this
    }
    fun parameter(parameter: String, value: Any?): UpdateOperation {
        builder.parameter(parameter,value)
        return this
    }

    fun toMono() = Mono.defer {
        Mono.just(builder.toPreparedStatement().ps.executeUpdate())
    }
}

class ResultSetState(
        val resultSetGetter: () -> ResultSet,
        val statement: PreparedStatement? = null,
        val connection: Connection? = null,
        val autoClose: Boolean
) {
    fun <T: Any> toFlux(mapper: (ResultSet) -> T): Flux<T> {
        return Flux.defer {
            val iterator = QueryIterator(this, resultSetGetter(), mapper, autoClose)
            Flux.fromIterable(iterator.asIterable())
                    .doOnTerminate { iterator.close() }
                    .doOnCancel { iterator.cancel() }
        }
    }

    fun <T: Any> toMono(mapper: (ResultSet) -> T) = Mono.defer {
        toFlux(mapper).single()
    }
}

class QueryIterator<out T>(val qs: ResultSetState,
						   val rs: ResultSet,
						   val mapper: (ResultSet) -> T,
						   val autoClose: Boolean
) : Iterator<T> {

    private var didNext = false
    private var hasNext = false
    private val cancelled = AtomicBoolean(false)

    override fun next(): T {
        if (!didNext) {
            rs.next()
        }
        didNext = false
        return mapper(rs)
    }

    override fun hasNext(): Boolean {
        if (cancelled.get()) {
            excecuteCancel()
            hasNext = false
            return false
        }
        if (!didNext) {
            hasNext = rs.next()
            didNext = true
        }
        return hasNext
    }

    fun asIterable() = object: Iterable<T> {
        override fun iterator(): Iterator<T> = this@QueryIterator
    }

    fun close() {
        rs.close()
        qs.statement?.close()
        if (autoClose)
            qs.connection?.close()
    }
    fun cancel() {
        cancelled.set(true)
    }
    private fun excecuteCancel() {
        rs.close()
        qs.statement?.close()
        if (autoClose)
            qs.connection?.close()
    }
}

fun PreparedStatement.processParameters(v: Array<out Any?>) = v.forEachIndexed { i,v2 -> processParameter(i,v2)}

fun PreparedStatement.processParameter(pos: Int, argVal: Any?) {
    when (argVal) {
        null -> setObject(pos + 1, null)
        is UUID -> setObject(pos + 1, argVal)
        is Int -> setInt(pos + 1, argVal)
        is String -> setString(pos + 1, argVal)
        is Double -> setDouble(pos + 1, argVal)
        is Boolean -> setBoolean(pos + 1, argVal)
        is Float -> setFloat(pos + 1, argVal)
        is Long -> setLong(pos + 1, argVal)
        is LocalTime -> setTime(pos + 1, java.sql.Time.valueOf(argVal))
        is LocalDate -> setDate(pos + 1, java.sql.Date.valueOf(argVal))
        is LocalDateTime -> setTimestamp(pos + 1, java.sql.Timestamp.valueOf(argVal))
        is BigDecimal -> setBigDecimal(pos + 1, argVal)
        is InputStream -> setBinaryStream(pos + 1, argVal)
        is Enum<*> -> setObject(pos + 1, argVal)
    }
}