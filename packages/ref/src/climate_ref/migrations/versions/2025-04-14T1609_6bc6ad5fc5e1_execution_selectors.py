"""execution-selectors

Revision ID: 6bc6ad5fc5e1
Revises: 904f2f2db24a
Create Date: 2025-04-14 16:09:07.685010

"""

from collections.abc import Sequence
from typing import Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "6bc6ad5fc5e1"
down_revision: Union[str, None] = "904f2f2db24a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("metric_execution_group", schema=None) as batch_op:
        batch_op.add_column(sa.Column("selectors", sa.JSON(), nullable=False))


def downgrade() -> None:
    with op.batch_alter_table("metric_execution_group", schema=None) as batch_op:
        batch_op.drop_column("selectors")
