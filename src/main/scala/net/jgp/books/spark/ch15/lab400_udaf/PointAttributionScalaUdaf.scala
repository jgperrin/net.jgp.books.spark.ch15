package net.jgp.books.spark.ch15.lab400_udaf

import java.util.ArrayList

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.slf4j.LoggerFactory

/**
 * Calculates loyalty points using a UDAF
 *
 * @author rambabu.posa
 */
@SerialVersionUID(-66830400L)
class PointAttributionScalaUdaf extends UserDefinedAggregateFunction {

  private val log = LoggerFactory.getLogger(classOf[PointAttributionScalaUdaf])
  val MAX_POINT_PER_ORDER = 3

  /**
   * Describes the schema of input sent to the UDAF. Spark UDAFs can operate
   * on any number of columns. In our use case, we only need one field.
   */
  override def inputSchema: StructType = {
    val inputFields = new ArrayList[StructField]
    inputFields.add(DataTypes.createStructField("_c0", DataTypes.IntegerType, true))
    DataTypes.createStructType(inputFields)
  }

  /**
   * Describes the schema of UDAF buffer.
   */
  override def bufferSchema: StructType = {
    val bufferFields = new ArrayList[StructField]
    bufferFields.add(DataTypes.createStructField("sum", DataTypes.IntegerType, true))
    DataTypes.createStructType(bufferFields)
  }

  /**
   * Datatype of the UDAF's output.
   */
  override def dataType: DataType = DataTypes.IntegerType

  /**
   * Describes whether the UDAF is deterministic or not.
   *
   * As Spark executes by splitting data, it processes the chunks separately
   * and combining them. If the UDAF logic is such that the result is
   * independent of the order in which data is processed and combined then
   * the UDAF is deterministic.
   */
  override def deterministic = true

  /**
   * Initializes the buffer. This method can be called any number of times
   * of Spark during processing.
   *
   * The contract should be that applying the merge function on two initial
   * buffers should just return the initial buffer itself, i.e.
   * `merge(initialBuffer, initialBuffer)` should equal `initialBuffer`.
   */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    log.trace("-> initialize() - buffer as {} row(s)", buffer.length)
    buffer.update(0, // column
      0) // value

    // You can repeat that for the number of columns you have in your
    // buffer
  }

  /**
   * Updates the buffer with an input row. Validations on input should be
   * performed in this method.
   */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    log.trace("-> update(), input row has {} args", input.length)
    if (input.isNullAt(0)) {
      log.trace("Value passed is null.")
      return
    }
    log.trace("-> update({}, {})", buffer.getInt(0), input.getInt(0))
    // Apply your business rule, could be in an external function/service.
    val initialValue = buffer.getInt(0)
    val inputValue = input.getInt(0)
    var outputValue = 0
    if (inputValue < MAX_POINT_PER_ORDER) outputValue = inputValue
    else outputValue = MAX_POINT_PER_ORDER
    outputValue += initialValue
    log.trace("Value passed to update() is {}, this will grant {} points", inputValue, outputValue)
    buffer.update(0, outputValue)
  }

  /**
   * Merges two aggregation buffers and stores the updated buffer values
   * back to buffer.
   */
  override def merge(buffer: MutableAggregationBuffer, row: Row): Unit = {
    log.trace("-> merge({}, {})", buffer.getInt(0), row.getInt(0))
    buffer.update(0, buffer.getInt(0) + row.getInt(0))
  }

  /**
   * Calculates the final result of this UDAF based on the given aggregation
   * buffer.
   */
  override def evaluate(row: Row): Integer = {
    log.trace("-> evaluate({})", row.getInt(0))
    row.getInt(0)
  }

}
