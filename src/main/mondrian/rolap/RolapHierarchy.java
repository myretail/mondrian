/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.mdx.*;
import mondrian.olap.*;
import mondrian.olap.Role.HierarchyAccess;
import mondrian.olap.fun.*;
import mondrian.olap.type.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RestrictedMemberReader.MultiCardinalityDefaultMember;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.SqlQuery;
import mondrian.spi.CellFormatter;
import mondrian.spi.impl.Scripts;

import org.apache.log4j.Logger;

import org.olap4j.impl.NamedListImpl;
import org.olap4j.metadata.NamedList;

import java.io.PrintWriter;
import java.util.*;

/**
 * <code>RolapHierarchy</code> implements {@link Hierarchy} for a ROLAP
 * database.
 *
 * <p>NOTE: This class must not contain any references to XML (MondrianDef)
 * objects. Put those in {@link mondrian.rolap.RolapSchemaLoader}.
 *
 * @author jhyde
 * @since 10 August, 2001
  */
public class RolapHierarchy extends HierarchyBase {

    private static final Logger LOGGER = Logger.getLogger(RolapHierarchy.class);

    protected RolapMember nullMember;

    private Exp aggregateChildrenExpression;

    /**
     * The level that the null member belongs too.
     */
    protected RolapLevel nullLevel;
    protected RolapLevel allLevel;

    /**
     * The 'all' member of this hierarchy. This exists even if the hierarchy
     * does not officially have an 'all' member.
     */
    protected RolapMemberBase allMember;
    private static final int ALL_LEVEL_CARDINALITY = 1;
    private static final int NULL_LEVEL_CARDINALITY = 1;
    final RolapAttribute attribute;
    private final Larder larder;
    public final RolapHierarchy closureFor;

    final NamedList<RolapLevel> levelList = new NamedListImpl<RolapLevel>();

    /** Whether this hierarchy is the Scenario hierarchy of its cube. */
    public final boolean isScenario;

    /**
     * Creates a RolapHierarchy.
     *
     * @param dimension Dimension this hierarchy belongs to
     * @param subName Name of hierarchy, or null if it is the same as the
     *     dimension
     * @param uniqueName Unique name of hierarchy
     * @param hasAll Whether the dimension has an 'all' level
     * @param attribute Attribute this is a hierarchy for; or null
     */
    RolapHierarchy(
        RolapDimension dimension,
        String subName,
        String uniqueName,
        boolean visible,
        boolean hasAll,
        RolapHierarchy closureFor,
        RolapAttribute attribute,
        Larder larder)
    {
        super(dimension, subName, uniqueName, visible, hasAll);
        this.attribute = attribute;
        this.larder = larder;
        this.closureFor = closureFor;
        this.isScenario = subName != null && subName.equals("Scenario");
        assert !isScenario
            || dimension.getDimensionType()
            == org.olap4j.metadata.Dimension.Type.SCENARIO;
    }

    void initHierarchy(
        RolapSchemaLoader schemaLoader,
        String allLevelName)
    {
        if (this instanceof RolapCubeHierarchy) {
            throw new AssertionError();
        }

        // Create an 'all' level even if the hierarchy does not officially
        // have one.
        allLevel =
            new RolapLevel(
                this,
                Util.first(allLevelName, "(All)"),
                true,
                0,
                ALL_ATTRIBUTE.inDimension(getDimension()),
                null,
                Collections.<RolapSchema.PhysColumn>emptyList(),
                null,
                null,
                RolapLevel.HideMemberCondition.Never,
                Larders.EMPTY,
                schemaLoader.resourceMap);
        if (hasAll) {
            this.levelList.add(allLevel);
        }

        // The null member belongs to a level with very similar properties to
        // the 'all' level.
        this.nullLevel =
            new RolapLevel(
                this,
                Util.first(allLevelName, "(All)"),
                true,
                0,
                NULL_ATTRIBUTE.inDimension(getDimension()),
                null,
                Collections.<RolapSchema.PhysColumn>emptyList(),
                null,
                null,
                RolapLevel.HideMemberCondition.Never,
<<<<<<< HEAD
                LevelType.Regular, ALL_LEVEL_CARDINALITY,
                Collections.<String, Annotation>emptyMap());
        allLevel.init(xmlCubeDimension);
        this.allMember = new RolapMemberBase(
            null, allLevel, RolapUtil.sqlNullValue,
            allMemberName, Member.MemberType.ALL);
        // assign "all member" caption
        if (xmlHierarchy.allMemberCaption != null
            && xmlHierarchy.allMemberCaption.length() > 0)
        {
            this.allMember.setCaption(xmlHierarchy.allMemberCaption);
        }
        this.allMember.setOrdinal(0);

        if (xmlHierarchy.levels.length == 0) {
            throw MondrianResource.instance().HierarchyHasNoLevels.ex(
                getUniqueName());
        }

        Set<String> levelNameSet = new HashSet<String>();
        for (MondrianDef.Level level : xmlHierarchy.levels) {
            if (!levelNameSet.add(level.name)) {
                throw MondrianResource.instance().HierarchyLevelNamesNotUnique
                    .ex(
                        getUniqueName(), level.name);
            }
        }

        // If the hierarchy has an 'all' member, the 'all' level is level 0.
        if (hasAll) {
            this.levels = new RolapLevel[xmlHierarchy.levels.length + 1];
            this.levels[0] = allLevel;
            for (int i = 0; i < xmlHierarchy.levels.length; i++) {
                final MondrianDef.Level xmlLevel = xmlHierarchy.levels[i];
                if (xmlLevel.getKeyExp() == null
                    && xmlHierarchy.memberReaderClass == null)
                {
                    throw MondrianResource.instance()
                        .LevelMustHaveNameExpression.ex(xmlLevel.name);
                }
                levels[i + 1] = new RolapLevel(this, i + 1, xmlLevel);
            }
        } else {
            this.levels = new RolapLevel[xmlHierarchy.levels.length];
            for (int i = 0; i < xmlHierarchy.levels.length; i++) {
                levels[i] = new RolapLevel(this, i, xmlHierarchy.levels[i]);
            }
        }

        if (xmlCubeDimension instanceof MondrianDef.DimensionUsage) {
            String sharedDimensionName =
                ((MondrianDef.DimensionUsage) xmlCubeDimension).source;
            this.sharedHierarchyName = sharedDimensionName;
            if (subName != null) {
                this.sharedHierarchyName += "." + subName; // e.g. "Time.Weekly"
            }
        } else {
            this.sharedHierarchyName = null;
        }
        if (xmlHierarchy.relation != null
            && xmlHierarchy.memberReaderClass != null)
        {
            throw MondrianResource.instance()
                .HierarchyMustNotHaveMoreThanOneSource.ex(getUniqueName());
        }
        if (!Util.isEmpty(xmlHierarchy.caption)) {
            setCaption(xmlHierarchy.caption);
        }
        defaultMemberName = xmlHierarchy.defaultMember;
    }
=======
                Larders.EMPTY,
                schemaLoader.resourceMap);
>>>>>>> upstream/4.0

        if (dimension.isMeasures()) {
            levelList.add(
                new RolapLevel(
                    this,
                    RolapSchemaLoader.MEASURES_LEVEL_NAME,
                    true,
                    levelList.size(),
                    MEASURES_ATTRIBUTE.inDimension(getDimension()),
                    null,
                    Collections.<RolapSchema.PhysColumn>emptyList(),
                    null,
                    null,
                    RolapLevel.HideMemberCondition.Never,
                    Larders.EMPTY,
                    schemaLoader.resourceMap));
        }
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RolapHierarchy)) {
            return false;
        }

        RolapHierarchy that = (RolapHierarchy)o;
        return getUniqueName().equals(that.getUniqueName());
    }

    public Larder getLarder() {
        return larder;
    }

    @Override
    public RolapDimension getDimension() {
        return (RolapDimension) dimension;
    }

    public final RolapSchema getRolapSchema() {
        return ((RolapDimension) dimension).schema;
    }

    public RolapMember getDefaultMember() {
        throw new UnsupportedOperationException();
    }

    public RolapMember getNullMember() {
        return nullMember;
    }

    /**
     * Returns the 'all' member.
     */
    public RolapMember getAllMember() {
        return allMember;
    }

    public Member createMember(
        Member parent,
        Level level,
        String name,
        Formula formula)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Adds to the FROM clause of the query the tables necessary to access the
     * members of this hierarchy in an inverse join order, used with agg tables.
     * If <code>expression</code> is not null, adds the tables necessary to
     * compute that expression.
     *
     * <p> This method is idempotent: if you call it more than once, it only
     * adds the table(s) to the FROM clause once.
     *
     * @param query Query to add the hierarchy to
     * @param expression Level to qualify up to; if null, qualifies up to the
     *    topmost ('all') expression, which may require more columns and more
     *    joins
     */
    void addToFromInverse(SqlQuery query, RolapSchema.PhysExpr expression) {
/*
        if (relation == null) {
            throw Util.newError(
                "cannot add hierarchy " + getUniqueName()
                + " to query: it does not have a <Table>, <View> or <Join>");
        }
        final boolean failIfExists = false;
        Mondrian3Def.RelationOrJoin subRelation = relation;
                TODO:
        if (relation instanceof MondrianDef.Join) {
            if (expression != null) {
                subRelation =
                    relationSubsetInverse(relation, expression.getTableAlias());
            }
        }
<<<<<<< HEAD
        query.addFrom(subRelation, null, failIfExists);
    }

    /**
     * Adds to the FROM clause of the query the tables necessary to access the
     * members of this hierarchy. If <code>expression</code> is not null, adds
     * the tables necessary to compute that expression.
     *
     * <p> This method is idempotent: if you call it more than once, it only
     * adds the table(s) to the FROM clause once.
     *
     * @param query Query to add the hierarchy to
     * @param expression Level to qualify up to; if null, qualifies up to the
     *    topmost ('all') expression, which may require more columns and more
     *    joins
     */
    void addToFrom(SqlQuery query, MondrianDef.Expression expression) {
        if (relation == null) {
            throw Util.newError(
                "cannot add hierarchy " + getUniqueName()
                + " to query: it does not have a <Table>, <View> or <Join>");
        }
        query.registerRootRelation(relation);
        final boolean failIfExists = false;
        MondrianDef.RelationOrJoin subRelation = relation;
        if (relation instanceof MondrianDef.Join) {
            if (expression != null) {
                // Suppose relation is
                //   (((A join B) join C) join D)
                // and the fact table is
                //   F
                // and our expression uses C. We want to make the expression
                //   F left join ((A join B) join C).
                // Search for the smallest subset of the relation which
                // uses C.
                subRelation =
                    relationSubset(relation, expression.getTableAlias());
            }
        }
=======
>>>>>>> upstream/4.0
        query.addFrom(
            subRelation,
            expression == null ? null : expression.getTableAlias(),
            failIfExists);
<<<<<<< HEAD
    }

    /**
     * Adds a table to the FROM clause of the query.
     * If <code>table</code> is not null, adds the table. Otherwise, add the
     * relation on which this hierarchy is based on.
     *
     * <p> This method is idempotent: if you call it more than once, it only
     * adds the table(s) to the FROM clause once.
     *
     * @param query Query to add the hierarchy to
     * @param table table to add to the query
     */
    void addToFrom(SqlQuery query, RolapStar.Table table) {
        if (getRelation() == null) {
            throw Util.newError(
                "cannot add hierarchy " + getUniqueName()
                + " to query: it does not have a <Table>, <View> or <Join>");
        }
        final boolean failIfExists = false;
        MondrianDef.RelationOrJoin subRelation = null;
        if (table != null) {
            // Suppose relation is
            //   (((A join B) join C) join D)
            // and the fact table is
            //   F
            // and the table to add is C. We want to make the expression
            //   F left join ((A join B) join C).
            // Search for the smallest subset of the relation which
            // joins with C.
            subRelation = lookupRelationSubset(getRelation(), table);
        }

        if (subRelation == null) {
            // If no table is found or specified, add the entire base relation.
            subRelation = getRelation();
        }

        boolean tableAdded =
            query.addFrom(
                subRelation,
                table != null ? table.getAlias() : null,
                failIfExists);
        if (tableAdded && table != null) {
            RolapStar.Condition joinCondition = table.getJoinCondition();
            if (joinCondition != null) {
                query.addWhere(joinCondition);
            }
        }
    }

    /**
     * Returns the smallest subset of <code>relation</code> which contains
     * the relation <code>alias</code>, or null if these is no relation with
     * such an alias, in inverse join order, used for agg tables.
     *
     * @param relation the relation in which to look for table by its alias
     * @param alias table alias to search for
     * @return the smallest containing relation or null if no matching table
     * is found in <code>relation</code>
     */
    private static MondrianDef.RelationOrJoin relationSubsetInverse(
        MondrianDef.RelationOrJoin relation,
        String alias)
    {
        if (relation instanceof MondrianDef.Relation) {
            MondrianDef.Relation table =
                (MondrianDef.Relation) relation;
            return table.getAlias().equals(alias)
                ? relation
                : null;

        } else if (relation instanceof MondrianDef.Join) {
            MondrianDef.Join join = (MondrianDef.Join) relation;
            MondrianDef.RelationOrJoin leftRelation =
                relationSubsetInverse(join.left, alias);
            return (leftRelation == null)
                ? relationSubsetInverse(join.right, alias)
                : join;

        } else {
            throw Util.newInternal("bad relation type " + relation);
        }
    }

    /**
     * Returns the smallest subset of <code>relation</code> which contains
     * the relation <code>alias</code>, or null if these is no relation with
     * such an alias.
     * @param relation the relation in which to look for table by its alias
     * @param alias table alias to search for
     * @return the smallest containing relation or null if no matching table
     * is found in <code>relation</code>
     */
    private static MondrianDef.RelationOrJoin relationSubset(
        MondrianDef.RelationOrJoin relation,
        String alias)
    {
        if (relation instanceof MondrianDef.Relation) {
            MondrianDef.Relation table =
                (MondrianDef.Relation) relation;
            return table.getAlias().equals(alias)
                ? relation
                : null;

        } else if (relation instanceof MondrianDef.Join) {
            MondrianDef.Join join = (MondrianDef.Join) relation;
            MondrianDef.RelationOrJoin rightRelation =
                relationSubset(join.right, alias);
            return (rightRelation == null)
                ? relationSubset(join.left, alias)
                : MondrianProperties.instance()
                    .FilterChildlessSnowflakeMembers.get()
                ? join
                : rightRelation;
        } else {
            throw Util.newInternal("bad relation type " + relation);
        }
    }

    /**
     * Returns the smallest subset of <code>relation</code> which contains
     * the table <code>targetTable</code>, or null if the targetTable is not
     * one of the joining table in <code>relation</code>.
     *
     * @param relation the relation in which to look for targetTable
     * @param targetTable table to add to the query
     * @return the smallest containing relation or null if no matching table
     * is found in <code>relation</code>
     */
    private static MondrianDef.RelationOrJoin lookupRelationSubset(
        MondrianDef.RelationOrJoin relation,
        RolapStar.Table targetTable)
    {
        if (relation instanceof MondrianDef.Table) {
            MondrianDef.Table table = (MondrianDef.Table) relation;
            if (table.name.equals(targetTable.getTableName())) {
                return relation;
            } else {
                // Not the same table if table names are different
                return null;
            }
        } else if (relation instanceof MondrianDef.Join) {
            // Search inside relation, starting from the rightmost table,
            // and move left along the join chain.
            MondrianDef.Join join = (MondrianDef.Join) relation;
            MondrianDef.RelationOrJoin rightRelation =
                lookupRelationSubset(join.right, targetTable);
            if (rightRelation == null) {
                // Keep searching left.
                return lookupRelationSubset(
                    join.left, targetTable);
            } else {
                // Found a match.
                return join;
            }
        }
        return null;
    }

    /**
     * Creates a member reader which enforces the access-control profile of
     * <code>role</code>.
     *
     * <p>This method may not be efficient, so the caller should take care
     * not to call it too often. A cache is a good idea.
     *
     * @param role Role
     * @return Member reader that implements access control
     *
     * @pre role != null
     * @post return != null
     */
    MemberReader createMemberReader(Role role) {
        final Access access = role.getAccess(this);
        switch (access) {
        case NONE:
            role.getAccess(this); // todo: remove
            throw Util.newInternal(
                "Illegal access to members of hierarchy " + this);
        case ALL:
            return (isRagged())
                ? new SmartRestrictedMemberReader(getMemberReader(), role)
                : getMemberReader();

        case CUSTOM:
            final Role.HierarchyAccess hierarchyAccess =
                role.getAccessDetails(this);
            final Role.RollupPolicy rollupPolicy =
                hierarchyAccess.getRollupPolicy();
            final NumericType returnType = new NumericType();
            switch (rollupPolicy) {
            case FULL:
                return new SmartRestrictedMemberReader(
                    getMemberReader(), role);
            case PARTIAL:
                Type memberType1 =
                    new mondrian.olap.type.MemberType(
                        getDimension(),
                        this,
                        null,
                        null);
                SetType setType = new SetType(memberType1);
                ListCalc listCalc =
                    new AbstractListCalc(
                        new DummyExp(setType), new Calc[0])
                    {
                        public TupleList evaluateList(
                            Evaluator evaluator)
                        {
                            return
                                new UnaryTupleList(
                                    getLowestMembersForAccess(
                                        evaluator, hierarchyAccess, null));
                        }

                        public boolean dependsOn(Hierarchy hierarchy) {
                            return true;
                        }
                    };
                final Calc partialCalc =
                    new LimitedRollupAggregateCalc(returnType, listCalc);

                final Exp partialExp =
                    new ResolvedFunCall(
                        new FunDefBase("$x", "x", "In") {
                            public Calc compileCall(
                                ResolvedFunCall call,
                                ExpCompiler compiler)
                            {
                                return partialCalc;
                            }

                            public void unparse(Exp[] args, PrintWriter pw) {
                                pw.print("$RollupAccessibleChildren()");
                            }
                        },
                        new Exp[0],
                        returnType);
                return new LimitedRollupSubstitutingMemberReader(
                    getMemberReader(), role, hierarchyAccess, partialExp);

            case HIDDEN:
                Exp hiddenExp =
                    new ResolvedFunCall(
                        new FunDefBase("$x", "x", "In") {
                            public Calc compileCall(
                                ResolvedFunCall call, ExpCompiler compiler)
                            {
                                return new ConstantCalc(returnType, null);
                            }

                            public void unparse(Exp[] args, PrintWriter pw) {
                                pw.print("$RollupAccessibleChildren()");
                            }
                        },
                        new Exp[0],
                        returnType);
                return new LimitedRollupSubstitutingMemberReader(
                    getMemberReader(), role, hierarchyAccess, hiddenExp);
            default:
                throw Util.unexpected(rollupPolicy);
            }
        default:
            throw Util.badValue(access);
        }
=======
*/
>>>>>>> upstream/4.0
    }

    /**
     * Goes recursively down a hierarchy and builds a list of the
     * members that should be constrained on because of access controls.
     * It isn't sufficient to constrain on the current level in the
     * evaluator because the actual constraint could be even more limited
     * <p>Example. If we only give access to Seattle but the query is on
     * the country level, we have to constrain at the city level, not state,
     * or else all the values of all cities in the state will be returned.
     */
<<<<<<< HEAD
    List<Member> getLowestMembersForAccess(
=======
    protected List<Member> getLowestMembersForAccess(
>>>>>>> upstream/4.0
        Evaluator evaluator,
        HierarchyAccess hAccess,
        Map<Member, Access> membersWithAccess)
    {
        if (membersWithAccess == null) {
            membersWithAccess =
                FunUtil.getNonEmptyMemberChildrenWithDetails(
                    evaluator,
                    ((RolapEvaluator) evaluator)
                        .getExpanding());
        }
        boolean goesLower = false;
        for (Member member : membersWithAccess.keySet()) {
            Access access = membersWithAccess.get(member);
            if (access == null) {
                access = hAccess.getAccess(member);
            }
            if (access != Access.ALL) {
                goesLower = true;
                break;
            }
        }
        if (goesLower) {
            // We still have to go one more level down.
            Map<Member, Access> newMap =
                new HashMap<Member, Access>();
            for (Member member : membersWithAccess.keySet()) {
                int savepoint = evaluator.savepoint();
                try {
                    evaluator.setContext(member);
                    newMap.putAll(
                        FunUtil.getNonEmptyMemberChildrenWithDetails(
                            evaluator,
                            member));
                } finally {
                    evaluator.restore(savepoint);
                }
            }
            // Now pass it recursively to this method.
<<<<<<< HEAD
            return getLowestMembersForAccess(
                evaluator, hAccess, newMap);
=======
            return getLowestMembersForAccess(evaluator, hAccess, newList);
>>>>>>> upstream/4.0
        }
        return new ArrayList<Member>(membersWithAccess.keySet());
    }

    /**
     * A hierarchy is ragged if it contains one or more levels with hidden
     * members.
     */
    public boolean isRagged() {
        for (RolapLevel level : levelList) {
            if (level.getHideMemberCondition()
                != RolapLevel.HideMemberCondition.Never)
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns an expression which will compute a member's value by aggregating
     * its children.
     *
     * <p>It is efficient to share one expression between all calculated members
     * in a parent-child hierarchy, so we only need need to validate the
     * expression once.
     */
    public synchronized Exp getAggregateChildrenExpression() {
        if (aggregateChildrenExpression == null) {
            UnresolvedFunCall fc = new UnresolvedFunCall(
                "$AggregateChildren",
                Syntax.Internal,
                new Exp[] {new HierarchyExpr(this)});
            Validator validator =
                    Util.createSimpleValidator(BuiltinFunTable.instance());
            aggregateChildrenExpression = fc.accept(validator);
        }
        return aggregateChildrenExpression;
    }

    public List<? extends RolapLevel> getLevelList() {
        return Util.cast(levelList);
    }

    /**
     * A <code>RolapNullMember</code> is the null member of its hierarchy.
     * Every hierarchy has precisely one. They are yielded by operations such as
     * <code>[Gender].[All].ParentMember</code>. Null members are usually
     * omitted from sets (in particular, in the set constructor operator "{ ...
     * }".
     */
    static class RolapNullMember extends RolapMemberBase {
        RolapNullMember(final RolapCubeLevel level) {
            super(
                null,
                level,
<<<<<<< HEAD
                RolapUtil.sqlNullValue,
                RolapUtil.mdxNullLiteral(),
                MemberType.NULL);
            assert level != null;
=======
                Util.COMPARABLE_EMPTY_LIST,
                MemberType.NULL,
                Util.makeFqName(
                    level.getHierarchy(), RolapUtil.mdxNullLiteral()),
                Larders.ofName(RolapUtil.mdxNullLiteral()));
>>>>>>> upstream/4.0
        }
    }

    /**
     * Calculated member which is also a measure (that is, a member of the
     * [Measures] dimension).
     */
    protected static class RolapCalculatedMeasure
        extends RolapCalculatedMember
        implements RolapMeasure
    {
        private RolapResult.ValueFormatter cellFormatter;

        public RolapCalculatedMeasure(
            RolapMember parent,
            RolapCubeLevel level,
            String name,
            Formula formula)
        {
            super(parent, level, name, formula);
        }

        public synchronized void setProperty(Property property, Object value) {
            if (property == Property.CELL_FORMATTER) {
                String cellFormatterClass = (String) value;
                try {
                    CellFormatter formatter =
                        RolapSchemaLoader.getFormatter(
                            cellFormatterClass,
                            CellFormatter.class,
                            null);
                    this.cellFormatter =
                        new RolapResult.CellFormatterValueFormatter(formatter);
                } catch (Exception e) {
                    throw MondrianResource.instance().CellFormatterLoadFailed
                        .ex(
                            cellFormatterClass, getUniqueName(), e);
                }
            }
            if (property == Property.CELL_FORMATTER_SCRIPT) {
                String language = (String) getPropertyValue(
                    Property.CELL_FORMATTER_SCRIPT_LANGUAGE);
                String scriptText = (String) value;
                try {
                    final Scripts.ScriptDefinition script =
                        new Scripts.ScriptDefinition(
                            scriptText,
                            Scripts.ScriptLanguage.lookup(language));
                    CellFormatter formatter =
                        RolapSchemaLoader.getFormatter(
                            null,
                            CellFormatter.class,
                            script);
                    this.cellFormatter =
                        new RolapResult.CellFormatterValueFormatter(formatter);
                } catch (Exception e) {
                    throw MondrianResource.instance().CellFormatterLoadFailed
                        .ex(
                            scriptText, getUniqueName(), e);
                }
            }
            super.setProperty(property, value);
        }

        public RolapResult.ValueFormatter getFormatter() {
            return cellFormatter;
        }
    }

    /**
     * Substitute for a member in a hierarchy whose rollup policy is 'partial'
     * or 'hidden'. The member is calculated using an expression which
     * aggregates only visible descendants.
     *
     * @see mondrian.olap.Role.RollupPolicy
     */
    public static class LimitedRollupMember extends DelegatingRolapMember {
        public final RolapMember member;
        private final Exp exp;
        final HierarchyAccess hierarchyAccess;

        LimitedRollupMember(
<<<<<<< HEAD
            RolapCubeMember member,
            Exp exp,
            HierarchyAccess hierarchyAccess)
        {
            super(
                member.getParentMember(),
                member.getRolapMember(),
                member.getLevel());
            this.hierarchyAccess = hierarchyAccess;
=======
            RolapMember member,
            Exp exp)
        {
            super(member);
>>>>>>> upstream/4.0
            assert !(member instanceof LimitedRollupMember);
            this.member = member;
            this.exp = exp;
        }

        public boolean equals(Object o) {
            return o instanceof LimitedRollupMember
                && ((LimitedRollupMember) o).member.equals(member);
        }

        public int hashCode() {
<<<<<<< HEAD
            int hash = member.hashCode();
            hash = Util.hash(hash, exp);
            return hash;
=======
            return member.hashCode();
>>>>>>> upstream/4.0
        }

        public Exp getExpression() {
            return exp;
        }

        @Override
        public Calc getCompiledExpression(RolapEvaluatorRoot root) {
            return root.getCompiled(getExpression(), true, null);
        }

        public boolean isCalculated() {
            return false;
        }

        public boolean isEvaluated() {
            return true;
        }
    }

    /**
     * Member reader which wraps a hierarchy's member reader, and if the
     * role has limited access to the hierarchy, replaces members with
     * dummy members which evaluate to the sum of only the accessible children.
     */
    static class LimitedRollupSubstitutingMemberReader
        extends SubstitutingMemberReader
    {
        private final Role.HierarchyAccess hierarchyAccess;
        private final Exp exp;

        /**
         * Creates a LimitedRollupSubstitutingMemberReader.
         *
         * @param memberReader Underlying member reader
         * @param role Role to enforce
         * @param hierarchyAccess Access this role has to the hierarchy
         * @param exp Expression for hidden member
         */
        public LimitedRollupSubstitutingMemberReader(
            MemberReader memberReader,
            Role role,
            Role.HierarchyAccess hierarchyAccess,
            Exp exp)
        {
            super(
                new SmartRestrictedMemberReader(
                    memberReader, role));
            this.hierarchyAccess = hierarchyAccess;
            this.exp = exp;
        }

<<<<<<< HEAD
        public Map<? extends Member, Access> getMemberChildren(
            RolapMember member,
            List<RolapMember> memberChildren,
            MemberChildrenConstraint constraint)
        {
            return memberReader.getMemberChildren(
                member,
                new SubstitutingMemberList(memberChildren),
                constraint);
        }

        public Map<? extends Member, Access> getMemberChildren(
            List<RolapMember> parentMembers,
            List<RolapMember> children,
            MemberChildrenConstraint constraint)
        {
            return memberReader.getMemberChildren(
                parentMembers,
                new SubstitutingMemberList(children),
                constraint);
        }

        public RolapMember substitute(RolapMember member, Access access) {
            if (member != null
                && member instanceof MultiCardinalityDefaultMember)
            {
                return new LimitedRollupMember(
                    (RolapCubeMember)
                        ((MultiCardinalityDefaultMember) member)
                            .member.getParentMember(),
                    exp,
                    hierarchyAccess);
            }
            if (member != null
                && (access == Access.CUSTOM || hierarchyAccess
                    .hasInaccessibleDescendants(member)))
            {
                // Member is visible, but at least one of its
                // descendants is not.
                if (member instanceof LimitedRollupMember) {
                    member = ((LimitedRollupMember) member).member;
                }
                return new LimitedRollupMember(
                    (RolapCubeMember) member,
                    exp,
                    hierarchyAccess);
=======
        @Override
        public RolapMember substitute(final RolapMember member) {
            if (member == null) {
                return null;
            }
            if (member instanceof MultiCardinalityDefaultMember) {
                return new LimitedRollupMember(
                    member.getParentMember(),
                    exp);
            }
            if (hierarchyAccess.getAccess(member) == Access.CUSTOM
                || hierarchyAccess.hasInaccessibleDescendants(member))
            {
                // Member is visible, but at least one of its
                // descendants is not.
                return new LimitedRollupMember(member, exp);
>>>>>>> upstream/4.0
            } else {
                // No need to substitute. Member and all of its
                // descendants are accessible. Total for member
                // is same as for FULL policy.
                return member;
            }
        }

        public RolapMember substitute(final RolapMember member) {
            if (member == null) {
                return null;
            }
            return substitute(member, hierarchyAccess.getAccess(member));
        }

        @Override
        public RolapMember desubstitute(RolapMember member) {
            if (member instanceof LimitedRollupMember) {
                return ((LimitedRollupMember) member).member;
            } else {
                return member;
            }
        }
    }

    /**
     * Compiled expression that computes rollup over a set of visible children.
     * The {@code listCalc} expression determines that list of children.
     */
    static class LimitedRollupAggregateCalc
        extends AggregateFunDef.AggregateCalc
    {
        public LimitedRollupAggregateCalc(
            Type returnType,
            ListCalc listCalc)
        {
            super(
                new DummyExp(returnType),
                listCalc,
                new ValueCalc(new DummyExp(returnType)));
        }
    }

    /**
     * Dummy element that acts as a namespace for resolving member names within
     * shared hierarchies. Acts like a cube that has a single child, the
     * hierarchy in question.
     */
    static class DummyElement implements OlapElement {
        private final RolapHierarchy hierarchy;

        DummyElement(RolapHierarchy hierarchy) {
            this.hierarchy = hierarchy;
        }

        public String getUniqueName() {
            throw new UnsupportedOperationException();
        }

        public String getName() {
            return "$";
        }

        public String getDescription() {
            throw new UnsupportedOperationException();
        }

        public OlapElement lookupChild(
            SchemaReader schemaReader,
            Id.Segment s,
            MatchType matchType)
        {
            if (!(s instanceof Id.NameSegment)) {
                return null;
            }
            final Id.NameSegment nameSegment = (Id.NameSegment) s;

            if (Util.equalName(nameSegment.name, hierarchy.dimension.getName()))
            {
                return hierarchy.dimension;
            }
            return null;
        }

        public String getQualifiedName() {
            throw new UnsupportedOperationException();
        }

        public String getCaption() {
            throw new UnsupportedOperationException();
        }

        public Hierarchy getHierarchy() {
            throw new UnsupportedOperationException();
        }

        public Dimension getDimension() {
            throw new UnsupportedOperationException();
        }

        public boolean isVisible() {
            throw new UnsupportedOperationException();
        }

        public String getLocalized(LocalizedProperty prop, Locale locale) {
            throw new UnsupportedOperationException();
        }
    }

    private static final RolapSharedAttribute ALL_ATTRIBUTE =
        new RolapSharedAttribute(
            "All",
            true,
            Collections.<RolapSchema.PhysColumn>emptyList(),
            null,
            null,
            Collections.<RolapSchema.PhysColumn>emptyList(),
            null,
            org.olap4j.metadata.Level.Type.ALL,
            ALL_LEVEL_CARDINALITY);

    private static final RolapSharedAttribute NULL_ATTRIBUTE =
        new RolapSharedAttribute(
            "Null",
            true,
            Collections.<RolapSchema.PhysColumn>emptyList(),
            null,
            null,
            Collections.<RolapSchema.PhysColumn>emptyList(),
            null,
            org.olap4j.metadata.Level.Type.NULL,
            NULL_LEVEL_CARDINALITY);

    private static final RolapSharedAttribute MEASURES_ATTRIBUTE =
        new RolapSharedAttribute(
            "Measures",
            true,
            Collections.<RolapSchema.PhysColumn>emptyList(),
            null,
            null,
            Collections.<RolapSchema.PhysColumn>emptyList(),
            null,
            org.olap4j.metadata.Level.Type.REGULAR,
            Integer.MIN_VALUE);
}

// End RolapHierarchy.java
