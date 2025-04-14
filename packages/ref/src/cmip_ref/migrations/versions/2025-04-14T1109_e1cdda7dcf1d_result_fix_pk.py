"""result-fix-pk

Revision ID: e1cdda7dcf1d
Revises: c5de99c14533
Create Date: 2025-04-14 11:09:40.820631

"""

from collections.abc import Sequence
from typing import Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e1cdda7dcf1d"
down_revision: Union[str, None] = "c5de99c14533"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    if op.get_context().dialect.name != "postgresql":
        with op.batch_alter_table("metric_execution_result", schema=None) as batch_op:
            batch_op.create_foreign_key(
                "fk_metric_execution_result_group_id",
                "metric_execution_group",
                ["metric_execution_group_id"],
                ["id"],
            )


def downgrade() -> None:
    if op.get_context().dialect.name != "postgresql":
        with op.batch_alter_table("metric_execution_result", schema=None) as batch_op:
            batch_op.drop_constraint("fk_metric_execution_result_group_id", type_="foreignkey")
