use common::catalog::Catalog;
use common::logical_plan::*;
use common::table::*;
use common::{get_name, CrustyError, DataType, Field, PredicateOp};
use sqlparser::ast::{
    BinaryOperator, Expr, Function, JoinConstraint, JoinOperator, SelectItem, SetExpr, TableFactor,
    Value,
};
use std::collections::HashSet;

/// Translates input to a LogicalPlan
/// Validates the columns and tables referenced using the catalog
/// Shares lifetime 'a with catalog
pub struct TranslateAndValidate<'a, T: Catalog> {
    /// Logical plan of operators encountered so far.
    plan: LogicalPlan,
    /// Catalog to validate the translations.
    catalog: &'a T,
    /// List of tables encountered. Used for field validation.
    tables: Vec<String>,
}

impl<'a, T: 'a + Catalog> TranslateAndValidate<'a, T> {
    /// Creates a new TranslateAndValidate object.
    fn new(catalog: &'a T) -> Self {
        Self {
            plan: LogicalPlan::new(),
            catalog,
            tables: Vec::new(),
        }
    }

    /// Given a column name, try to figure out what table it belongs to by looking through all of the tables.
    ///
    /// # Arguments
    ///
    /// * `identifiers` - a list of elements in a multi-part identifier e.g. table.column would be vec!["table", "column"]
    ///
    /// # Returns
    ///
    /// FieldIdent's of the form { table: table, column: table.column, alias: column }
    /// or { table: table, column: table.column} if the full identifier is passed.
    fn disambiguate_name(&self, identifiers: Vec<&str>) -> Result<FieldIdentifier, CrustyError> {
        let orig = identifiers.join(".");
        if identifiers.len() > 2 {
            return Err(CrustyError::ValidationError(format!(
                "No . table names supported in field {}",
                orig
            )));
        }
        if identifiers.len() == 2 {
            let table_id = Table::get_table_id(&identifiers[0]);
            if self.catalog.is_valid_column(table_id, &identifiers[1]) {
                return Ok(FieldIdentifier::new(&identifiers[0], &orig));
            }
            return Err(CrustyError::ValidationError(format!(
                "The field {} is not present in tables listed in the query",
                orig
            )));
        }

        let mut field = None;
        for table in &self.tables {
            let table_id = Table::get_table_id(table);
            if self.catalog.is_valid_column(table_id, &orig) {
                if field.is_some() {
                    return Err(CrustyError::ValidationError(format!(
                        "The field {} could refer to more than one table listed in the query",
                        orig
                    )));
                }
                let new_name = format!("{}.{}", table, orig);
                field = Some(FieldIdentifier::new_column_alias(table, &new_name, &orig));
            }
        }

        field.ok_or_else(|| {
            CrustyError::ValidationError(format!(
                "The field {} is not present in tables listed in the query",
                orig
            ))
        })
    }

    /// Translates a sqlparser::ast to a LogicalPlan.
    ///
    /// Validates the columns and tables referenced using the catalog.
    /// All table names referenced in from and join clauses are added to self.tables.
    ///
    /// # Arguments
    ///
    /// * `sql` - AST to transalte.
    /// * `catalog` - Catalog for validation.
    pub fn from_sql(sql: &sqlparser::ast::Query, catalog: &T) -> Result<LogicalPlan, CrustyError> {
        let mut translator = TranslateAndValidate::new(catalog);
        translator.process_query(sql)?;
        Ok(translator.plan)
    }

    /// Helper function to recursively process sqlparser::ast::Query
    ///
    /// # Arguments
    ///
    /// * `query` - AST to process.
    fn process_query(&mut self, query: &sqlparser::ast::Query) -> Result<(), CrustyError> {
        match &query.body {
            SetExpr::Select(b) => {
                let select = &*b;
                self.process_select(select)
            }
            SetExpr::Query(_) => {
                //TODO NOT HANDLED
                Err(CrustyError::ValidationError(String::from(
                    "Query ops not supported ",
                )))
            }
            SetExpr::SetOperation {
                op: _,
                all: _,
                left: _,
                right: _,
            } => {
                //TODO NOT HANDLED
                Err(CrustyError::ValidationError(String::from(
                    "Set operations not supported ",
                )))
            }
            SetExpr::Values(_) => {
                //TODO NOT HANDLED
                Err(CrustyError::ValidationError(String::from(
                    "Value operation not supported ",
                )))
            }
        }
    }

    /// Helper function to recursively process sqlparser::ast::Select
    ///
    /// # Arguments
    ///
    /// * `query` - AST of a select query to process.
    fn process_select(&mut self, select: &sqlparser::ast::Select) -> Result<(), CrustyError> {
        // Pointer to the current node.
        let mut node = None;

        // Distinct
        if select.distinct {
            //TODO NOT HANDLED
            return Err(CrustyError::ValidationError(String::from(
                "Distinct not supported ",
            )));
        }

        // Doesn't need the for loop rn but keeping for the future when cross products are supported.
        // From
        if select.from.len() > 1 {
            //TODO NOT HANDLED
            return Err(CrustyError::ValidationError(String::from(
                "Cross product not supported ",
            )));
        }
        for sel in &select.from {
            node = Some(self.process_table_factor(&sel.relation)?);
            // Join
            for join in &sel.joins {
                let join_node = self.process_join(&join, node.unwrap())?;
                node = Some(join_node);
            }
        }

        // Where
        if let Some(expr) = &select.selection {
            let predicate = self.process_binary_op(expr)?;
            // table references in filter
            let table = match (&predicate.left, &predicate.right) {
                (PredExpr::Literal(_), PredExpr::Ident(id)) => id.table().to_string(),
                (PredExpr::Ident(id), PredExpr::Literal(_)) => id.table().to_string(),
                _ => {
                    return Err(CrustyError::ValidationError(String::from("Only where predicates with at least one indentifier and at least one literal are supported")));
                }
            };
            let op = FilterNode { table, predicate };
            let idx = self.plan.add_node(LogicalOp::Filter(op));
            self.plan.add_edge(idx, node.unwrap());
            node = Some(idx);
        }

        if select.having.is_some() {
            //TODO NOT HANDLED
            return Err(CrustyError::ValidationError(String::from(
                "Having not supported",
            )));
        }

        // Select
        let mut fields = Vec::new();
        let mut has_agg = false;
        let mut wildcard = false;
        for item in &select.projection {
            let field = match item {
                SelectItem::Wildcard => {
                    if select.projection.len() > 1 {
                        return Err(CrustyError::ValidationError(String::from(
                            "Cannot select wildcard and exp in same select",
                        )));
                    }
                    wildcard = true;
                    break;
                }
                SelectItem::UnnamedExpr(expr) => self.expr_to_ident(expr)?,
                SelectItem::ExprWithAlias { expr, alias } => {
                    let mut field = self.expr_to_ident(expr)?;
                    field.set_alias(alias.to_string());
                    field
                }
                _ => {
                    //TODO NOT HANDLED
                    return Err(CrustyError::ValidationError(String::from(
                        "Select unsupported expression",
                    )));
                }
            };
            if field.agg_op().is_some() {
                has_agg = true;
            }
            fields.push(field);
        }

        // Aggregates and group by
        if has_agg {
            let mut group_by = Vec::new();
            {
                let mut group_set = HashSet::new();
                for expr in &select.group_by {
                    let col = match expr {
                        Expr::Identifier(name) => name,
                        _ => {
                            return Err(CrustyError::ValidationError(String::from(
                                "Group by unsupported expression",
                            )));
                        }
                    };
                    let field = self.disambiguate_name(vec![col])?;
                    group_set.insert(field.column().to_string());
                    group_by.push(field);
                }

                // Checks that only aggregates and group by fields are projected out
                for f in &fields {
                    if f.agg_op().is_none() && !group_set.contains(f.column()) {
                        return Err(CrustyError::ValidationError(format!(
                            "The expression '{}' must be part of an aggregate function or group by",
                            f.column()
                        )));
                    }
                }
            }
            let op = AggregateNode {
                fields: fields.clone(),
                group_by,
            };
            let idx = self.plan.add_node(LogicalOp::Aggregate(op));
            self.plan.add_edge(idx, node.unwrap());
            node = Some(idx);

            // Replace field column names with aliases to project
            fields = fields
                .iter()
                .map(|f| {
                    let name = f.alias().unwrap_or_else(|| f.column());
                    FieldIdentifier::new(f.table(), name)
                })
                .collect();
        }
        let identifiers = if wildcard {
            ProjectIdentifiers::Wildcard
        } else {
            ProjectIdentifiers::List(fields)
        };
        let op = ProjectNode { identifiers };
        let idx = self.plan.add_node(LogicalOp::Project(op));
        self.plan.add_edge(idx, node.unwrap());
        Ok(())
    }

    /// Creates a corresponding LogicalOp, adds it to self.plan, and returns the OpIndex.
    ///
    /// Helper function to process sqlparser::ast::TableFactor.
    ///
    /// # Arguments
    ///
    /// * `tf` - Table to process.
    fn process_table_factor(
        &mut self,
        tf: &sqlparser::ast::TableFactor,
    ) -> Result<OpIndex, CrustyError> {
        match tf {
            TableFactor::Table { name, .. } => {
                let name = get_name(&name)?;
                let table_id = Table::get_table_id(&name);
                if !self.catalog.is_valid_table(table_id) {
                    return Err(CrustyError::ValidationError(String::from(
                        "Invalid table name",
                    )));
                }
                self.tables.push(name.clone());
                let op = ScanNode { alias: name };
                Ok(self.plan.add_node(LogicalOp::Scan(op)))
            }
            _ => Err(CrustyError::ValidationError(String::from(
                "Nested joins and derived tables not supported",
            ))),
        }
    }

    /// Returns the name of the table from the node, if the node is a table level operator, like scan. Otherwise, return none.
    ///
    /// # Arguments
    ///
    /// * `node` - Node to get the table name from.
    fn get_table_alias_from_op(&self, node: OpIndex) -> Option<String> {
        match &self.plan.get_operator(node)? {
            LogicalOp::Scan(ScanNode { alias }) => Some(alias.clone()),
            _ => None,
        }
    }

    /// Parses sqlparser::ast::Join into a Join LogicalOp, adds the Op to
    /// logical plan, and returns OpIndex of the join node.
    ///
    /// # Arguments
    ///
    /// * `join` - The join node to parse.
    /// * `left_table_node` - Node containing the left table to join.
    fn process_join(
        &mut self,
        join: &sqlparser::ast::Join,
        left_table_node: OpIndex,
    ) -> Result<OpIndex, CrustyError> {
        let right_table_node = self.process_table_factor(&join.relation)?;
        let jc = match &join.join_operator {
            JoinOperator::Inner(jc) => jc,
            _ => {
                return Err(CrustyError::ValidationError(String::from(
                    "Unsupported join type",
                )));
            }
        };

        if let JoinConstraint::On(expr) = jc {
            let pred = self.process_binary_op(expr)?;
            let left = pred
                .left
                .ident()
                .ok_or_else(|| {
                    CrustyError::ValidationError(String::from("Invalid join predicate"))
                })?
                .clone();
            let right = pred
                .right
                .ident()
                .ok_or_else(|| {
                    CrustyError::ValidationError(String::from("Invalid join predicate"))
                })?
                .clone();
            let op = JoinNode {
                left,
                right,
                op: pred.op,
                left_table: self.get_table_alias_from_op(left_table_node),
                right_table: self.get_table_alias_from_op(right_table_node),
            };
            let idx = self.plan.add_node(LogicalOp::Join(op));
            self.plan.add_edge(idx, right_table_node);
            self.plan.add_edge(idx, left_table_node);
            return Ok(idx);
        }
        Err(CrustyError::ValidationError(String::from(
            "Unsupported join type",
        )))
    }

    /// Parses an expression to a predicate node.
    ///
    /// # Arguments
    ///
    /// * `expr` - Expression to parse.
    fn process_binary_op(&self, expr: &Expr) -> Result<PredicateNode, CrustyError> {
        match expr {
            Expr::BinaryOp { left, op, right } => Ok(PredicateNode {
                left: self.expr_to_pred_expr(left)?,
                right: self.expr_to_pred_expr(right)?,
                op: Self::binary_operator_to_predicate(op)?,
            }),
            _ => Err(CrustyError::ValidationError(String::from(
                "Unsupported binary operation",
            ))),
        }
    }

    /// Parses the non-operator parts of the expression to predicate expressions.
    ///
    /// # Arguments
    ///
    /// * `expr` - Non-operator part of the expression to parse.
    fn expr_to_pred_expr(&self, expr: &Expr) -> Result<PredExpr, CrustyError> {
        match expr {
            Expr::Value(val) => match val {
                Value::Number(s) => {
                    let i = s.parse::<i32>().map_err(|_| {
                        CrustyError::ValidationError(format!("Unsupported literal {}", s))
                    })?;
                    let f = Field::IntField(i);
                    Ok(PredExpr::Literal(f))
                }
                Value::SingleQuotedString(s) => {
                    let f = Field::StringField(s.to_string());
                    Ok(PredExpr::Literal(f))
                }
                _ => Err(CrustyError::ValidationError(String::from(
                    "Unsupported literal in predicate",
                ))),
            },
            _ => Ok(PredExpr::Ident(self.expr_to_ident(expr)?)),
        }
    }

    /// Prases binary operator to predicate operators.
    ///
    /// # Arguments
    ///
    /// * `op` - Binary operator to parse.
    fn binary_operator_to_predicate(op: &BinaryOperator) -> Result<PredicateOp, CrustyError> {
        match op {
            BinaryOperator::Gt => Ok(PredicateOp::GreaterThan),
            BinaryOperator::Lt => Ok(PredicateOp::LessThan),
            BinaryOperator::GtEq => Ok(PredicateOp::GreaterThanOrEq),
            BinaryOperator::LtEq => Ok(PredicateOp::LessThanOrEq),
            BinaryOperator::Eq => Ok(PredicateOp::Equals),
            BinaryOperator::NotEq => Ok(PredicateOp::NotEq),
            _ => Err(CrustyError::ValidationError(String::from(
                "Unsupported binary operation",
            ))),
        }
    }

    /// Validates that an aggregate operation is valid for the type of field.
    ///
    /// Field must
    /// * be disambiguated so that field.column() returns a str of the form table.column
    /// * have an associated op
    ///
    /// # Arguments
    ///
    /// * `field` - Field to be aggregated.
    fn validate_aggregate(&self, field: &FieldIdentifier) -> Result<(), CrustyError> {
        let split_field: Vec<&str> = field.column().split('.').collect();
        if field.agg_op().is_none() || split_field.len() != 2 {
            return Ok(());
        }
        let table_name = field.table();
        let col_name = split_field[1];
        let alias = field.alias().unwrap_or_else(|| field.column());
        let op = field.agg_op().unwrap();
        let table_id = Table::get_table_id(table_name);
        let schema = self.catalog.get_table_schema(table_id)?;
        let attr = schema
            .get_attribute(*schema.get_field_index(col_name).unwrap())
            .unwrap();

        match attr.dtype() {
            DataType::Int => Ok(()),
            DataType::String => match op {
                AggOp::Count | AggOp::Max | AggOp::Min => Ok(()),
                _ => Err(CrustyError::ValidationError(format!(
                    "Cannot perform operation {} on field {}",
                    op, alias,
                ))),
            },
        }
    }

    /// Converts a sqparser::ast::Expr to a LogicalOp::FieldIdent.
    ///
    /// # Arguments
    ///
    /// * `expr` - Expression to be converted.
    fn expr_to_ident(&self, expr: &Expr) -> Result<FieldIdentifier, CrustyError> {
        match expr {
            Expr::Identifier(name) => self.disambiguate_name(vec![name]),
            Expr::CompoundIdentifier(names) => {
                self.disambiguate_name(names.iter().map(|s| s.as_ref()).collect())
            }
            Expr::Function(Function { name, args, .. }) => {
                let op = match &get_name(name)?.to_uppercase()[..] {
                    "AVG" => AggOp::Avg,
                    "COUNT" => AggOp::Count,
                    "MAX" => AggOp::Max,
                    "MIN" => AggOp::Min,
                    "SUM" => AggOp::Sum,
                    _ => {
                        return Err(CrustyError::ValidationError(String::from(
                            "Unsupported SQL function",
                        )));
                    }
                };
                if args.is_empty() || args.len() > 1 {
                    return Err(CrustyError::ValidationError(format!(
                        "Wrong number of args in {} operation",
                        name
                    )));
                }
                let mut field = match &args[0] {
                    Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                        self.expr_to_ident(&args[0])?
                    }
                    _ => {
                        return Err(CrustyError::ValidationError(String::from(
                            "Aggregate over unsupported expression",
                        )));
                    }
                };
                field.set_op(op);
                field.default_alias();
                self.validate_aggregate(&field)?;
                Ok(field)
            }
            _ => Err(CrustyError::ValidationError(String::from(
                "Unsupported expression",
            ))),
        }
    }
}

