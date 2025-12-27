"""Command-line interface for schema translator."""

from pathlib import Path
from typing import Optional

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

from .core.config import settings
from .core.discovery import SchemaDiscoverer
from .core.llm_mapper import LLMMapper
from .core.resolver import MappingResolver
from .core.transforms import DataTransformer
from .shared.logging import logger, setup_logging

app = typer.Typer(
    name="schema-translator",
    help="LLM-powered semantic schema mapping for heterogeneous tenant data"
)
console = Console()

# Initialize components
discoverer = SchemaDiscoverer(
    schemas_dir=settings.customer_schemas_dir,
    samples_dir=settings.customer_samples_dir
)


@app.command()
def discover(
    tenant: str = typer.Argument(..., help="Tenant identifier"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output")
):
    """Discover and profile schema for a tenant."""
    
    if verbose:
        setup_logging(level="DEBUG")
    
    console.print(f"[bold blue]Discovering schema for tenant: {tenant}[/bold blue]")
    
    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            
            # Discover schema
            task1 = progress.add_task("Discovering schema...", total=None)
            schema_tables = discoverer.discover_tenant_schema(tenant)
            progress.update(task1, completed=True)
            
            # Profile columns
            task2 = progress.add_task("Profiling columns...", total=None)
            column_profiles = discoverer.profile_tenant_columns(tenant)
            progress.update(task2, completed=True)
        
        # Display results
        _display_schema_discovery(tenant, schema_tables, column_profiles)
        
    except Exception as e:
        console.print(f"[red]Error discovering schema: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def propose(
    tenant: str = typer.Argument(..., help="Tenant identifier"),
    field: Optional[str] = typer.Option(None, "--field", help="Focus on specific canonical field"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output")
):
    """Generate mapping proposals for a tenant."""
    
    if verbose:
        setup_logging(level="DEBUG")
    
    console.print(f"[bold blue]Generating mapping proposals for tenant: {tenant}[/bold blue]")
    
    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            
            # Initialize components
            task1 = progress.add_task("Initializing LLM mapper...", total=None)
            llm_mapper = LLMMapper()
            resolver = MappingResolver(llm_mapper)
            progress.update(task1, completed=True)
            
            # Profile columns
            task2 = progress.add_task("Profiling columns...", total=None)
            column_profiles = discoverer.profile_tenant_columns(tenant)
            progress.update(task2, completed=True)
            
            # Generate mappings
            task3 = progress.add_task("Generating mappings...", total=None)
            mappings = resolver.resolve_batch_mappings(column_profiles)
            progress.update(task3, completed=True)
        
        # Filter by field if specified
        if field:
            mappings = [m for m in mappings if m.canonical_field == field]
            if not mappings:
                console.print(f"[yellow]No mappings found for field '{field}'[/yellow]")
                return
        
        # Display results
        _display_mapping_proposals(tenant, mappings, column_profiles)
        
    except Exception as e:
        console.print(f"[red]Error generating proposals: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def apply(
    tenant: str = typer.Argument(..., help="Tenant identifier"),
    threshold: Optional[float] = typer.Option(None, "--threshold", help="Auto-approval threshold"),
    output_dir: Optional[Path] = typer.Option(None, "--output", help="Output directory"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output")
):
    """Apply transformations for a tenant."""
    
    if verbose:
        setup_logging(level="DEBUG")
    
    console.print(f"[bold blue]Applying transformations for tenant: {tenant}[/bold blue]")
    
    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            
            # Initialize components
            task1 = progress.add_task("Initializing components...", total=None)
            llm_mapper = LLMMapper()
            resolver = MappingResolver(llm_mapper)
            transformer = DataTransformer()
            progress.update(task1, completed=True)
            
            # Override threshold if provided
            if threshold:
                resolver.auto_accept_threshold = threshold
            
            # Profile and map
            task2 = progress.add_task("Generating mappings...", total=None)
            column_profiles = discoverer.profile_tenant_columns(tenant)
            mappings = resolver.resolve_batch_mappings(column_profiles)
            progress.update(task2, completed=True)
            
            # Create mapping plan
            from .shared.models import MappingPlan
            mapping_plan = MappingPlan(
                tenant=tenant,
                version="1.0",
                canonical_schema_version="1.0",
                mappings=mappings,
                coverage_stats={
                    "total_columns": len(mappings),
                    "accepted_mappings": len([m for m in mappings if m.status == "accepted"]),
                    "hitl_required": len([m for m in mappings if m.status == "hitl_required"]),
                    "rejected": len([m for m in mappings if m.status == "rejected"])
                }
            )
            
            # Load source data
            task3 = progress.add_task("Loading source data...", total=None)
            source_data = _load_tenant_data(tenant)
            progress.update(task3, completed=True)
            
            # Apply transformations
            task4 = progress.add_task("Applying transformations...", total=None)
            result = transformer.apply_mapping_plan(tenant, mapping_plan, source_data)
            progress.update(task4, completed=True)
        
        # Display results
        _display_transform_results(tenant, result, mapping_plan)
        
    except Exception as e:
        console.print(f"[red]Error applying transformations: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def explain(
    tenant: str = typer.Argument(..., help="Tenant identifier"),
    field: str = typer.Argument(..., help="Canonical field to explain"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output")
):
    """Explain how a canonical field was mapped."""
    
    if verbose:
        setup_logging(level="DEBUG")
    
    console.print(f"[bold blue]Explaining mapping for {tenant}.{field}[/bold blue]")
    
    try:
        # Initialize components
        llm_mapper = LLMMapper()
        resolver = MappingResolver(llm_mapper)
        
        # Generate mappings
        column_profiles = discoverer.profile_tenant_columns(tenant)
        mappings = resolver.resolve_batch_mappings(column_profiles)
        
        # Find mappings for the field
        field_mappings = [m for m in mappings if m.canonical_field == field]
        
        if not field_mappings:
            console.print(f"[yellow]No mappings found for field '{field}' in tenant {tenant}[/yellow]")
            return
        
        # Display explanation
        _display_field_explanation(tenant, field, field_mappings, column_profiles)
        
    except Exception as e:
        console.print(f"[red]Error explaining mapping: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def list_tenants():
    """List available tenants."""
    
    console.print("[bold blue]Available tenants:[/bold blue]")
    
    try:
        table = Table(title="Tenants")
        table.add_column("Tenant", style="cyan")
        table.add_column("Tables", justify="right")
        table.add_column("Columns", justify="right")
        table.add_column("Sample Data", style="green")
        
        for tenant_dir in settings.customer_schemas_dir.iterdir():
            if tenant_dir.is_dir() and (tenant_dir / "schema.yaml").exists():
                tenant_name = tenant_dir.name
                
                try:
                    schema_tables = discoverer.discover_tenant_schema(tenant_name)
                    table_count = len(schema_tables)
                    column_count = sum(len(cols) for cols in schema_tables.values())
                    
                    # Check sample data
                    sample_dir = settings.customer_samples_dir / tenant_name
                    has_samples = sample_dir.exists() and any(sample_dir.glob("*.csv"))
                    
                    table.add_row(
                        tenant_name,
                        str(table_count),
                        str(column_count),
                        "✓" if has_samples else "✗"
                    )
                    
                except Exception as e:
                    table.add_row(tenant_name, "Error", str(e), "?")
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[red]Error listing tenants: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def serve(
    host: str = typer.Option("0.0.0.0", help="Host to bind to"),
    port: int = typer.Option(8000, help="Port to bind to"),
    reload: bool = typer.Option(False, help="Enable auto-reload")
):
    """Start the FastAPI server."""
    
    console.print(f"[bold blue]Starting Schema Translator API on {host}:{port}[/bold blue]")
    
    try:
        import uvicorn
        uvicorn.run(
            "app.main:app",
            host=host,
            port=port,
            reload=reload,
            log_level="info"
        )
    except ImportError:
        console.print("[red]uvicorn not installed. Install with: pip install uvicorn[/red]")
        raise typer.Exit(1)


def _display_schema_discovery(tenant: str, schema_tables: dict, column_profiles: list):
    """Display schema discovery results."""
    
    # Summary
    total_tables = len(schema_tables)
    total_columns = sum(len(cols) for cols in schema_tables.values())
    
    console.print(Panel(
        f"[bold]Tenant:[/bold] {tenant}\n"
        f"[bold]Tables:[/bold] {total_tables}\n"
        f"[bold]Columns:[/bold] {total_columns}",
        title="Schema Discovery Summary"
    ))
    
    # Tables and columns
    for table_name, columns in schema_tables.items():
        table = Table(title=f"Table: {table_name}")
        table.add_column("Column", style="cyan")
        table.add_column("Type", style="yellow")
        table.add_column("Inferred Type", style="green")
        table.add_column("Samples", style="dim")
        
        for column in columns:
            # Find profile for this column
            profile = next(
                (p for p in column_profiles if p.source_column.column == column.column),
                None
            )
            
            inferred_type = profile.inferred_type.value if profile else "unknown"
            samples = ", ".join(profile.sample_values[:3]) if profile else "no data"
            
            table.add_row(
                column.column,
                column.type or "unknown",
                inferred_type,
                samples
            )
        
        console.print(table)
        console.print()


def _display_mapping_proposals(tenant: str, mappings: list, column_profiles: list):
    """Display mapping proposals."""
    
    # Summary
    auto_accepted = len([m for m in mappings if m.status == "accepted"])
    hitl_required = len([m for m in mappings if m.status == "hitl_required"])
    rejected = len([m for m in mappings if m.status == "rejected"])
    
    console.print(Panel(
        f"[bold]Tenant:[/bold] {tenant}\n"
        f"[bold green]Auto-accepted:[/bold green] {auto_accepted}\n"
        f"[bold yellow]HITL required:[/bold yellow] {hitl_required}\n"
        f"[bold red]Rejected:[/bold red] {rejected}",
        title="Mapping Proposals Summary"
    ))
    
    # Detailed mappings
    table = Table(title="Mapping Proposals")
    table.add_column("Source Column", style="cyan")
    table.add_column("Canonical Field", style="green")
    table.add_column("Status", style="yellow")
    table.add_column("Confidence", justify="right", style="blue")
    table.add_column("Justification", style="dim")
    
    for mapping in mappings:
        source_col = f"{mapping.source_column.table}.{mapping.source_column.column}"
        canonical_field = mapping.canonical_field or "None"
        confidence = f"{mapping.mapping_score.final_score:.3f}" if mapping.mapping_score else "0.000"
        
        # Get justification from LLM response
        justification = "No justification"
        if mapping.llm_response and mapping.llm_response.proposed_mappings:
            justification = mapping.llm_response.proposed_mappings[0].justification[:50] + "..."
        
        # Color status
        status_color = {
            "accepted": "[green]accepted[/green]",
            "hitl_required": "[yellow]hitl_required[/yellow]",
            "rejected": "[red]rejected[/red]"
        }.get(mapping.status, mapping.status)
        
        table.add_row(
            source_col,
            canonical_field,
            status_color,
            confidence,
            justification
        )
    
    console.print(table)


def _display_transform_results(tenant: str, result, mapping_plan):
    """Display transformation results."""
    
    console.print(Panel(
        f"[bold]Tenant:[/bold] {tenant}\n"
        f"[bold]Output:[/bold] {result.output_path}\n"
        f"[bold]Rows Processed:[/bold] {result.rows_processed}\n"
        f"[bold]Rows Successful:[/bold] {result.rows_successful}\n"
        f"[bold]Errors:[/bold] {len(result.errors)}",
        title="Transform Results"
    ))
    
    if result.errors:
        console.print("[bold red]Errors:[/bold red]")
        for error in result.errors:
            console.print(f"  • {error}")
    
    # Coverage stats
    stats = mapping_plan.coverage_stats
    console.print(f"\n[bold]Coverage Statistics:[/bold]")
    console.print(f"  • Total columns: {stats.get('total_columns', 0)}")
    console.print(f"  • Accepted mappings: {stats.get('accepted_mappings', 0)}")
    console.print(f"  • HITL required: {stats.get('hitl_required', 0)}")
    console.print(f"  • Rejected: {stats.get('rejected', 0)}")


def _display_field_explanation(tenant: str, field: str, mappings: list, column_profiles: list):
    """Display detailed explanation for a field mapping."""
    
    console.print(Panel(
        f"[bold]Tenant:[/bold] {tenant}\n"
        f"[bold]Canonical Field:[/bold] {field}",
        title="Field Mapping Explanation"
    ))
    
    for mapping in mappings:
        source_col = f"{mapping.source_column.table}.{mapping.source_column.column}"
        
        console.print(f"\n[bold cyan]Source Column:[/bold cyan] {source_col}")
        console.print(f"[bold]Status:[/bold] {mapping.status}")
        
        if mapping.mapping_score:
            console.print(f"[bold]Final Score:[/bold] {mapping.mapping_score.final_score:.3f}")
            console.print("[bold]Score Breakdown:[/bold]")
            console.print(f"  • LLM Confidence: {mapping.mapping_score.llm_confidence:.3f}")
            console.print(f"  • Name Similarity: {mapping.mapping_score.name_similarity:.3f}")
            console.print(f"  • Type Compatibility: {mapping.mapping_score.type_compatibility:.3f}")
            console.print(f"  • Value Range Match: {mapping.mapping_score.value_range_match:.3f}")
        
        if mapping.llm_response and mapping.llm_response.proposed_mappings:
            proposal = mapping.llm_response.proposed_mappings[0]
            console.print(f"[bold]Justification:[/bold] {proposal.justification}")
            if proposal.assumptions:
                console.print(f"[bold]Assumptions:[/bold]")
                for assumption in proposal.assumptions:
                    console.print(f"  • {assumption}")
        
        # Show sample values
        profile = next(
            (p for p in column_profiles if p.source_column.column == mapping.source_column.column),
            None
        )
        if profile and profile.sample_values:
            console.print(f"[bold]Sample Values:[/bold] {', '.join(profile.sample_values[:5])}")


def _load_tenant_data(tenant: str) -> dict:
    """Load tenant data for transformations."""
    data = {}
    tenant_sample_dir = settings.customer_samples_dir / tenant
    
    for csv_file in tenant_sample_dir.glob("*.csv"):
        table_name = csv_file.stem
        df = pd.read_csv(csv_file)
        data[table_name] = df
    
    return data


if __name__ == "__main__":
    app()
