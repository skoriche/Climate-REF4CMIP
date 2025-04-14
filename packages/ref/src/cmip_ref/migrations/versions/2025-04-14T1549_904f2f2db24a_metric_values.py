"""metric-values

Revision ID: 904f2f2db24a
Revises: e1cdda7dcf1d
Create Date: 2025-04-14 15:49:07.952333

"""

from collections.abc import Sequence
from typing import Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "904f2f2db24a"
down_revision: Union[str, None] = "e1cdda7dcf1d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "metric_value",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("metric_execution_result_id", sa.Integer(), nullable=False),
        sa.Column("value", sa.Float(), nullable=False),
        sa.Column("attributes", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("(CURRENT_TIMESTAMP)"), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("(CURRENT_TIMESTAMP)"), nullable=False),
        sa.ForeignKeyConstraint(
            ["metric_execution_result_id"],
            ["metric_execution_result.id"],
            name=op.f("fk_metric_value_metric_execution_result_id_metric_execution_result"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_metric_value")),
    )


def downgrade() -> None:
    op.drop_table("metric_value")
