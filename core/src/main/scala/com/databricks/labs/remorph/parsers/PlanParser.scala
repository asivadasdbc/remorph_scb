package com.databricks.labs.remorph.parsers

import com.databricks.labs.remorph.intermediate.{ParsingErrors, PlanGenerationFailure, TranspileFailure}
import com.databricks.labs.remorph.transpilers.{SourceCode}
import com.databricks.labs.remorph.{intermediate => ir}
import com.databricks.labs.remorph.{Result, WorkflowStage}
import com.databricks.labs.remorph.{KoResult, Result, OkResult, WorkflowStage, intermediate => ir}
import com.databricks.labs.remorph.transpilers.SourceCode
import org.antlr.v4.runtime._
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

import scala.util.control.NonFatal

trait PlanParser[P <: Parser] {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  protected def createLexer(input: CharStream): Lexer
  protected def createParser(stream: TokenStream): P
  protected def createTree(parser: P): ParserRuleContext
  protected def createPlan(tree: ParserRuleContext): ir.LogicalPlan
  protected def addErrorStrategy(parser: P): Unit
  def dialect: String

  // TODO: This is probably not where the optimizer should be as this is a Plan "Parser" - it is here for now
  protected def createOptimizer: ir.Rules[ir.LogicalPlan]

  /**
   * Parse the input source code into a Parse tree
   * @param input The source code with filename
   * @return Returns a parse tree on success otherwise a description of the errors
   */
  def parse(input: SourceCode): Result[ParserRuleContext] = {
    val inputString = CharStreams.fromString(input.source)
    val lexer = createLexer(inputString)
    val tokenStream = new CommonTokenStream(lexer)
    val parser = createParser(tokenStream)
    addErrorStrategy(parser)
    val errListener = new ProductionErrorCollector(input.source, input.filename)
    parser.removeErrorListeners()
    parser.addErrorListener(errListener)
    val tree = createTree(parser)
    if (errListener.errorCount > 0) {
      KoResult(stage = WorkflowStage.PARSE, ParsingErrors(errListener.errors))
    } else {
      OkResult(tree)
    }
  }

  /**
   * Visit the parse tree and create a logical plan
   * @param tree The parse tree
   * @return Returns a logical plan on success otherwise a description of the errors
   */
  def visit(tree: ParserRuleContext): Result[ir.LogicalPlan] = {
    try {
      val plan = createPlan(tree)
      OkResult(plan)
    } catch {
      case NonFatal(e) =>
        KoResult(stage = WorkflowStage.PLAN, PlanGenerationFailure(e))
    }
  }

  // TODO: This is probably not where the optimizer should be as this is a Plan "Parser" - it is here for now
  /**
   * Optimize the logical plan
   *
   * @param logicalPlan The logical plan
   * @return Returns an optimized logical plan on success otherwise a description of the errors
   */
  def optimize(logicalPlan: ir.LogicalPlan): Result[ir.LogicalPlan] = {
    try {
      val plan = createOptimizer.apply(logicalPlan)
      OkResult(plan)
    } catch {
      case NonFatal(e) =>
        KoResult(stage = WorkflowStage.OPTIMIZE, TranspileFailure(e))
    }
  }
}
