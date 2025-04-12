"""metric-result-fix-pk

Revision ID: 4dacd370801b
Revises: c5de99c14533
Create Date: 2025-04-12 13:44:21.378996

"""

from collections.abc import Sequence
from typing import Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "4dacd370801b"
down_revision: Union[str, None] = "c5de99c14533"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("metric_execution_result", schema=None) as batch_op:
        batch_op.create_foreign_key(
            "fk_metric_execution_result_group_id",
            "metric_execution_group",
            ["metric_execution_group_id"],
            ["id"],
        )


def downgrade() -> None:
    with op.batch_alter_table("metric_execution_result", schema=None) as batch_op:
        batch_op.drop_constraint("fk_metric_execution_result_group_id", type_="foreignkey")
